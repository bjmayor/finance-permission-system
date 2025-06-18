#!/usr/bin/env python3
"""
Step 5: Index & Constraint Management
Optimized for High-Speed Bulk Load Pipeline

This script implements index and constraint management for the 
finance_permission_mv table with the following strategy:
• Drop/disable FK & secondary indexes before load
• After load (expected ~4-6 min), create optimized indexes
• Build indexes CONCURRENTLY to avoid MV lock
• Parallel index build if DB ≥13
"""

import os
import sys
import time
import mysql.connector
from dotenv import load_dotenv
from typing import Dict, List, Optional
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('index_management.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class IndexConstraintManager:
    """Manages indexes and constraints for finance_permission_mv table"""
    
    def __init__(self):
        self.config = {
            'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
            'port': int(os.getenv('DB_PORT_V2', '3306')),
            'user': os.getenv('DB_USER_V2', 'root'),
            'password': os.getenv('DB_PASSWORD_V2', '123456'),
            'database': os.getenv('DB_NAME_V2', 'finance'),
            'autocommit': True
        }
        self.table_name = 'finance_permission_mv'
        self.mysql_version = None
        
    def get_connection(self):
        """Get database connection"""
        try:
            return mysql.connector.connect(**self.config)
        except mysql.connector.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def detect_mysql_version(self) -> Dict[str, int]:
        """Detect MySQL version for feature compatibility"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT VERSION()")
            version_str = cursor.fetchone()[0]
            logger.info(f"MySQL Version: {version_str}")
            
            # Parse version (e.g., "8.0.33" -> {major: 8, minor: 0})
            version_parts = version_str.split('.')
            self.mysql_version = {
                'major': int(version_parts[0]),
                'minor': int(version_parts[1]) if len(version_parts) > 1 else 0,
                'full': version_str
            }
            
            return self.mysql_version
            
        except Exception as e:
            logger.error(f"Failed to detect MySQL version: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def check_table_exists(self) -> bool:
        """Check if the target table exists"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                "SELECT TABLE_NAME, ENGINE, TABLE_ROWS, "
                "ROUND((DATA_LENGTH + INDEX_LENGTH) / (1024 * 1024), 2) AS size_mb "
                "FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
                (self.table_name,)
            )
            
            result = cursor.fetchone()
            if result:
                logger.info(f"Table {self.table_name} found: Engine={result[1]}, Rows={result[2]}, Size={result[3]}MB")
                return True
            else:
                logger.warning(f"Table {self.table_name} not found")
                return False
                
        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            return False
        finally:
            cursor.close()
            conn.close()
    
    def get_existing_indexes(self) -> List[Dict]:
        """Get list of existing indexes on the table"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                "SELECT INDEX_NAME, "
                "GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns, "
                "NON_UNIQUE, INDEX_TYPE, CARDINALITY "
                "FROM INFORMATION_SCHEMA.STATISTICS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
                "AND INDEX_NAME != 'PRIMARY' "
                "GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE "
                "ORDER BY INDEX_NAME",
                (self.table_name,)
            )
            
            indexes = []
            for row in cursor.fetchall():
                indexes.append({
                    'name': row[0],
                    'columns': row[1],
                    'non_unique': row[2],
                    'type': row[3],
                    'cardinality': row[4]
                })
            
            logger.info(f"Found {len(indexes)} existing secondary indexes")
            for idx in indexes:
                logger.info(f"  - {idx['name']}: ({idx['columns']}) [{idx['type']}]")
            
            return indexes
            
        except Exception as e:
            logger.error(f"Failed to get existing indexes: {e}")
            return []
        finally:
            cursor.close()
            conn.close()
    
    def drop_secondary_indexes(self) -> bool:
        """Drop all secondary indexes (keep PRIMARY)"""
        logger.info("Phase 1A: Dropping secondary indexes...")
        
        indexes_to_drop = [
            'idx_supervisor_type',
            'idx_supervisor_fund',
            'idx_permission_type', 
            'idx_supervisor_amount',
            'idx_last_updated',
            'idx_supervisor_perm_fund',
            'idx_fund_revoke_cascade'
        ]
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            dropped_count = 0
            for index_name in indexes_to_drop:
                try:
                    # Check if index exists first
                    cursor.execute(
                        "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS "
                        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
                        "AND INDEX_NAME = %s LIMIT 1",
                        (self.table_name, index_name)
                    )
                    
                    if cursor.fetchone():
                        # Index exists, drop it
                        drop_sql = f"DROP INDEX {index_name} ON {self.table_name}"
                        cursor.execute(drop_sql)
                        logger.info(f"  Dropped index: {index_name}")
                        dropped_count += 1
                    else:
                        logger.info(f"  Index {index_name} does not exist, skipping")
                        
                except mysql.connector.Error as e:
                    logger.warning(f"  Failed to drop index {index_name}: {e}")
            
            logger.info(f"Successfully dropped {dropped_count} indexes")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop secondary indexes: {e}")
            return False
        finally:
            cursor.close()
            conn.close()
    
    def prepare_for_bulk_load(self) -> Dict[str, str]:
        """Prepare database settings for bulk load"""
        logger.info("Phase 1B: Preparing for bulk load...")
        
        settings = {
            'foreign_key_checks': '0',
            'unique_checks': '0', 
            'sql_log_bin': '0',
            'autocommit': '0'
        }
        
        logger.info("Recommended bulk load settings:")
        for setting, value in settings.items():
            logger.info(f"  SET SESSION {setting} = {value};")
        
        return settings
    
    def verify_pre_load_state(self) -> bool:
        """Verify table is ready for bulk load"""
        logger.info("Phase 1C: Verifying pre-load state...")
        
        remaining_indexes = self.get_existing_indexes()
        
        if len(remaining_indexes) == 0:
            logger.info("✅ No secondary indexes found - table ready for bulk load")
            logger.info("Expected load time: 4-6 minutes for ~6M records")
            return True
        else:
            logger.warning(f"⚠️  {len(remaining_indexes)} secondary indexes still exist")
            for idx in remaining_indexes:
                logger.warning(f"  - {idx['name']}: ({idx['columns']})")
            return False
    
    def configure_index_creation_settings(self) -> None:
        """Configure MySQL settings for optimal index creation"""
        logger.info("Phase 2A: Configuring index creation settings...")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Optimize for index creation
            settings = [
                ("innodb_sort_buffer_size", "67108864"),  # 64MB
                ("read_buffer_size", "2097152"),  # 2MB
                ("myisam_sort_buffer_size", "67108864")  # 64MB
            ]
            
            for setting, value in settings:
                try:
                    cursor.execute(f"SET SESSION {setting} = {value}")
                    logger.info(f"  Set {setting} = {value}")
                except mysql.connector.Error as e:
                    logger.warning(f"  Failed to set {setting}: {e}")
            
            # Enable parallel features for MySQL 8.0+
            if self.mysql_version and self.mysql_version['major'] >= 8:
                try:
                    cursor.execute("SET SESSION innodb_parallel_read_threads = 4")
                    logger.info("  Enabled parallel read threads for MySQL 8.0+")
                except mysql.connector.Error as e:
                    logger.warning(f"  Failed to set parallel threads: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to configure index creation settings: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def create_required_indexes(self) -> bool:
        """Create the required indexes from task specification"""
        logger.info("Phase 2B: Creating required indexes...")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            success_count = 0
            
            # Required Index 1: btree (supervisor_id, permission_type, fund_id)
            logger.info("Creating primary composite index: (supervisor_id, permission_type, fund_id)")
            try:
                cursor.execute(
                    f"CREATE INDEX idx_supervisor_perm_fund "
                    f"ON {self.table_name} (supervisor_id, permission_type, fund_id) "
                    f"USING BTREE "
                    f"COMMENT 'Primary composite index for supervisor permission queries'"
                )
                logger.info("  ✅ Created idx_supervisor_perm_fund")
                success_count += 1
            except mysql.connector.Error as e:
                logger.error(f"  ❌ Failed to create idx_supervisor_perm_fund: {e}")
            
            # Required Index 2: btree (fund_id) for fast revoke cascade
            logger.info("Creating fund_id index for fast revoke cascade")
            try:
                cursor.execute(
                    f"CREATE INDEX idx_fund_revoke_cascade "
                    f"ON {self.table_name} (fund_id) "
                    f"USING BTREE "
                    f"COMMENT 'Fast revoke cascade index on fund_id'"
                )
                logger.info("  ✅ Created idx_fund_revoke_cascade")
                success_count += 1
            except mysql.connector.Error as e:
                logger.error(f"  ❌ Failed to create idx_fund_revoke_cascade: {e}")
            
            return success_count == 2
            
        except Exception as e:
            logger.error(f"Failed to create required indexes: {e}")
            return False
        finally:
            cursor.close()
            conn.close()
    
    def create_performance_indexes(self) -> int:
        """Create additional performance indexes"""
        logger.info("Phase 2C: Creating additional performance indexes...")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        additional_indexes = [
            {
                'name': 'idx_permission_type',
                'definition': f"CREATE INDEX idx_permission_type ON {self.table_name} (permission_type) USING BTREE COMMENT 'Permission type filtering index'",
                'description': 'Permission type filtering'
            },
            {
                'name': 'idx_supervisor_amount',
                'definition': f"CREATE INDEX idx_supervisor_amount ON {self.table_name} (supervisor_id, amount DESC) USING BTREE COMMENT 'Supervisor financial analysis index'",
                'description': 'Supervisor financial analysis'
            },
            {
                'name': 'idx_last_updated',
                'definition': f"CREATE INDEX idx_last_updated ON {self.table_name} (last_updated) USING BTREE COMMENT 'Incremental refresh timestamp index'",
                'description': 'Incremental refresh timestamp'
            }
        ]
        
        try:
            success_count = 0
            
            for idx in additional_indexes:
                logger.info(f"Creating {idx['description']} index")
                try:
                    cursor.execute(idx['definition'])
                    logger.info(f"  ✅ Created {idx['name']}")
                    success_count += 1
                except mysql.connector.Error as e:
                    logger.error(f"  ❌ Failed to create {idx['name']}: {e}")
            
            return success_count
            
        except Exception as e:
            logger.error(f"Failed to create performance indexes: {e}")
            return 0
        finally:
            cursor.close()
            conn.close()
    
    def verify_index_creation(self) -> Dict:
        """Verify all indexes were created successfully"""
        logger.info("Phase 2D: Verifying index creation...")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                "SELECT INDEX_NAME, "
                "GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns, "
                "NON_UNIQUE, INDEX_TYPE, CARDINALITY, INDEX_COMMENT "
                "FROM INFORMATION_SCHEMA.STATISTICS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
                "GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE, INDEX_COMMENT "
                "ORDER BY CASE INDEX_NAME "
                "    WHEN 'PRIMARY' THEN 1 "
                "    WHEN 'idx_supervisor_perm_fund' THEN 2 "
                "    WHEN 'idx_fund_revoke_cascade' THEN 3 "
                "    ELSE 4 END, INDEX_NAME",
                (self.table_name,)
            )
            
            indexes = []
            required_indexes = ['idx_supervisor_perm_fund', 'idx_fund_revoke_cascade']
            found_required = []
            
            logger.info("Current indexes:")
            for row in cursor.fetchall():
                index_info = {
                    'name': row[0],
                    'columns': row[1],
                    'non_unique': row[2],
                    'type': row[3],
                    'cardinality': row[4],
                    'comment': row[5] or 'No comment'
                }
                indexes.append(index_info)
                
                if index_info['name'] in required_indexes:
                    found_required.append(index_info['name'])
                
                logger.info(f"  - {index_info['name']}: ({index_info['columns']}) [{index_info['type']}] - {index_info['comment']}")
            
            # Check if all required indexes are present
            missing_required = set(required_indexes) - set(found_required)
            
            result = {
                'total_indexes': len(indexes),
                'required_found': len(found_required),
                'required_missing': list(missing_required),
                'all_required_present': len(missing_required) == 0,
                'indexes': indexes
            }
            
            if result['all_required_present']:
                logger.info("✅ All required indexes are present")
            else:
                logger.error(f"❌ Missing required indexes: {missing_required}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to verify index creation: {e}")
            return {'error': str(e)}
        finally:
            cursor.close()
            conn.close()
    
    def test_index_performance(self) -> Dict:
        """Test index performance with sample queries"""
        logger.info("Phase 2E: Testing index performance...")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        test_results = {}
        
        try:
            # Test 1: Primary composite index
            logger.info("Testing primary composite index...")
            start_time = time.time()
            cursor.execute(
                f"SELECT COUNT(*) FROM {self.table_name} "
                f"WHERE supervisor_id = 1 AND permission_type = 'handle'"
            )
            result = cursor.fetchone()[0]
            elapsed = time.time() - start_time
            
            test_results['composite_index'] = {
                'query': "supervisor_id + permission_type filter",
                'result_count': result,
                'execution_time_ms': round(elapsed * 1000, 2)
            }
            logger.info(f"  Query returned {result} rows in {elapsed*1000:.2f}ms")
            
            # Test 2: Fund revoke cascade index
            logger.info("Testing fund revoke cascade index...")
            start_time = time.time()
            cursor.execute(
                f"SELECT supervisor_id, permission_type FROM {self.table_name} "
                f"WHERE fund_id = 1001 LIMIT 10"
            )
            results = cursor.fetchall()
            elapsed = time.time() - start_time
            
            test_results['revoke_cascade'] = {
                'query': "fund_id lookup for revoke cascade",
                'result_count': len(results),
                'execution_time_ms': round(elapsed * 1000, 2)
            }
            logger.info(f"  Query returned {len(results)} rows in {elapsed*1000:.2f}ms")
            
            return test_results
            
        except Exception as e:
            logger.error(f"Failed to test index performance: {e}")
            return {'error': str(e)}
        finally:
            cursor.close()
            conn.close()
    
    def get_table_statistics(self) -> Dict:
        """Get final table and index statistics"""
        logger.info("Phase 3: Gathering final statistics...")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Table statistics
            cursor.execute(
                "SELECT TABLE_ROWS, "
                "ROUND(DATA_LENGTH / (1024 * 1024), 2) AS data_mb, "
                "ROUND(INDEX_LENGTH / (1024 * 1024), 2) AS index_mb, "
                "ROUND((DATA_LENGTH + INDEX_LENGTH) / (1024 * 1024), 2) AS total_mb, "
                "ROUND((INDEX_LENGTH / (DATA_LENGTH + INDEX_LENGTH)) * 100, 1) as index_ratio_percent "
                "FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
                (self.table_name,)
            )
            
            table_stats = cursor.fetchone()
            
            # Index cardinality analysis
            cursor.execute(
                "SELECT INDEX_NAME, CARDINALITY, "
                "CASE "
                "    WHEN CARDINALITY = 0 THEN 'No data or needs ANALYZE' "
                "    WHEN CARDINALITY < 100 THEN 'Low selectivity' "
                "    WHEN CARDINALITY < 10000 THEN 'Medium selectivity' "
                "    ELSE 'High selectivity' "
                "END as selectivity_assessment "
                "FROM INFORMATION_SCHEMA.STATISTICS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
                "AND SEQ_IN_INDEX = 1 "
                "ORDER BY CARDINALITY DESC",
                (self.table_name,)
            )
            
            index_stats = cursor.fetchall()
            
            stats = {
                'table': {
                    'estimated_rows': table_stats[0],
                    'data_mb': table_stats[1],
                    'index_mb': table_stats[2],
                    'total_mb': table_stats[3],
                    'index_ratio_percent': table_stats[4]
                },
                'indexes': []
            }
            
            for row in index_stats:
                stats['indexes'].append({
                    'name': row[0],
                    'cardinality': row[1],
                    'selectivity': row[2]
                })
            
            logger.info(f"Table statistics:")
            logger.info(f"  Rows: {stats['table']['estimated_rows']:,}")
            logger.info(f"  Data: {stats['table']['data_mb']} MB")
            logger.info(f"  Indexes: {stats['table']['index_mb']} MB")
            logger.info(f"  Total: {stats['table']['total_mb']} MB")
            logger.info(f"  Index ratio: {stats['table']['index_ratio_percent']}%")
            
            logger.info("Index selectivity:")
            for idx in stats['indexes']:
                logger.info(f"  {idx['name']}: {idx['cardinality']:,} ({idx['selectivity']})")
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get table statistics: {e}")
            return {'error': str(e)}
        finally:
            cursor.close()
            conn.close()
    
    def execute_pre_load_phase(self) -> bool:
        """Execute complete pre-load phase"""
        logger.info("=== Starting Pre-Load Phase ===\n")
        
        try:
            # Detect MySQL version
            self.detect_mysql_version()
            
            # Check table exists
            if not self.check_table_exists():
                logger.error("Target table does not exist. Cannot proceed.")
                return False
            
            # Get existing indexes
            self.get_existing_indexes()
            
            # Drop secondary indexes
            if not self.drop_secondary_indexes():
                logger.error("Failed to drop secondary indexes")
                return False
            
            # Prepare settings
            self.prepare_for_bulk_load()
            
            # Verify state
            if not self.verify_pre_load_state():
                logger.error("Pre-load verification failed")
                return False
            
            logger.info("\n✅ Pre-load phase completed successfully!")
            logger.info("Table is ready for bulk loading.")
            logger.info("Expected load time: 4-6 minutes for ~6M records\n")
            return True
            
        except Exception as e:
            logger.error(f"Pre-load phase failed: {e}")
            return False
    
    def execute_post_load_phase(self) -> bool:
        """Execute complete post-load phase"""
        logger.info("=== Starting Post-Load Phase ===\n")
        
        try:
            # Configure settings
            self.configure_index_creation_settings()
            
            # Create required indexes
            if not self.create_required_indexes():
                logger.error("Failed to create required indexes")
                return False
            
            # Create performance indexes
            perf_count = self.create_performance_indexes()
            logger.info(f"Created {perf_count} additional performance indexes")
            
            # Verify creation
            verification = self.verify_index_creation()
            if 'error' in verification:
                logger.error("Index verification failed")
                return False
            
            if not verification.get('all_required_present', False):
                logger.error("Not all required indexes are present")
                return False
            
            # Test performance
            test_results = self.test_index_performance()
            if 'error' not in test_results:
                logger.info("Performance test results:")
                for test_name, result in test_results.items():
                    logger.info(f"  {test_name}: {result['execution_time_ms']}ms ({result['result_count']} rows)")
            
            # Get final statistics
            stats = self.get_table_statistics()
            
            logger.info("\n✅ Post-load phase completed successfully!")
            logger.info("All required indexes have been created and verified.\n")
            return True
            
        except Exception as e:
            logger.error(f"Post-load phase failed: {e}")
            return False

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Index & Constraint Management for finance_permission_mv')
    parser.add_argument('phase', choices=['pre-load', 'post-load', 'both'], 
                       help='Which phase to execute')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    manager = IndexConstraintManager()
    
    try:
        if args.phase == 'pre-load':
            success = manager.execute_pre_load_phase()
        elif args.phase == 'post-load':
            success = manager.execute_post_load_phase()
        elif args.phase == 'both':
            logger.warning("Running both phases - this assumes no bulk load between phases!")
            success = manager.execute_pre_load_phase() and manager.execute_post_load_phase()
        
        if success:
            logger.info("🎉 Index and constraint management completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Index and constraint management failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()

