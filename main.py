from typing import List, Dict, Set
from abc import ABC, abstractmethod
import datetime

# 模拟数据库表结构
class User:
    def __init__(self, id: int, name: str, role: str, department: str, parent_id: int = None):
        self.id = id
        self.name = name
        self.role = role
        self.department = department
        self.parent_id = parent_id

class FinancialFund:
    def __init__(self, fund_id: int, handle_by: int, order_id: int, customer_id: int, amount: float):
        self.fund_id = fund_id
        self.handle_by = handle_by
        self.order_id = order_id
        self.customer_id = customer_id
        self.amount = amount

class Order:
    def __init__(self, order_id: int, user_id: int):
        self.order_id = order_id
        self.user_id = user_id

class Customer:
    def __init__(self, customer_id: int, admin_user_id: int):
        self.customer_id = customer_id
        self.admin_user_id = admin_user_id

# 权限服务
class PermissionService:
    def __init__(self):
        self.users = {
            1: User(1, "超级管理员", "admin", "总部"),
            2: User(2, "财务主管", "supervisor", "华东区", 1),
            3: User(3, "财务专员", "staff", "华东区", 2),
            4: User(4, "财务专员", "staff", "华南区", 1)
        }

        self.financial_funds = [
            FinancialFund(1001, 3, 2001, 3001, 50000),
            FinancialFund(1002, 2, 2002, 3002, 80000),
            FinancialFund(1003, 3, 2003, 3003, 60000)
        ]

        self.orders = {
            2001: Order(2001, 3),
            2002: Order(2002, 2),
            2003: Order(2003, 3)
        }

        self.customers = {
            3001: Customer(3001, 3),
            3002: Customer(3002, 2),
            3003: Customer(3003, 3)
        }

    def get_subordinates(self, user_id: int) -> Set[int]:
        """递归获取所有下属ID"""
        subordinates = set()
        stack = [user_id]

        while stack:
            current = stack.pop()
            subordinates.add(current)
            for uid, user in self.users.items():
                if user.parent_id == current:
                    stack.append(uid)
        return subordinates

    def get_accessible_data_scope(self, user: User) -> Dict:
        """获取数据权限范围"""
        scope = {"handle_by": set(), "order_ids": set(), "customer_ids": set()}

        if user.role == "admin":
            scope["handle_by"] = {u.id for u in self.users.values()}
            scope["order_ids"] = {o.order_id for o in self.orders.values()}
            scope["customer_ids"] = {c.customer_id for c in self.customers.values()}
        elif user.role == "supervisor":
            subordinates = self.get_subordinates(user.id)
            scope["handle_by"] = {u.id for u in self.users.values() if u.id in subordinates}
            scope["order_ids"] = {o.order_id for o in self.orders.values()
                                if self.users[o.user_id].id in subordinates}
            scope["customer_ids"] = {c.customer_id for c in self.customers.values()
                                if self.users[c.admin_user_id].id in subordinates}
        elif user.role == "staff":
            scope["handle_by"] = {user.id}
            scope["order_ids"] = {o.order_id for o in self.orders.values()
                                if o.user_id == user.id}
            scope["customer_ids"] = {c.customer_id for c in self.customers.values()
                                if c.admin_user_id == user.id}

        return scope

# 财务服务
class FinancialService:
    def __init__(self, permission_svc: PermissionService):
        self.permission_svc = permission_svc

    def get_funds(self, user: User) -> List[FinancialFund]:
        """获取财务列表"""
        scope = self.permission_svc.get_accessible_data_scope(user)

        filtered_funds = []
        for fund in self.permission_svc.financial_funds:
            if (fund.handle_by in scope["handle_by"] or
                fund.order_id in scope["order_ids"] or
                fund.customer_id in scope["customer_ids"]):
                filtered_funds.append(fund)
        return filtered_funds

# 模拟API网关
class ApiGateway:
    def __init__(self):
        self.permission_svc = PermissionService()
        self.financial_svc = FinancialService(self.permission_svc)

        self.current_user = None

    def authenticate(self, role: str):
        """模拟用户认证"""
        self.current_user = next(
            (u for u in self.permission_svc.users.values() if u.role == role),
            self.permission_svc.users[1]  # 默认超级管理员
        )

    def get_funds(self):
        """获取财务数据API"""
        if not self.current_user:
            raise Exception("请先登录")

        return self.financial_svc.get_funds(self.current_user)

# 测试演示
if __name__ == "__main__":
    gateway = ApiGateway()

    print("=== 超管视角 ===")
    gateway.authenticate("admin")
    for fund in gateway.get_funds():
        print(f"超管查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")

    print("\n=== 主管视角 ===")
    gateway.authenticate("supervisor")
    for fund in gateway.get_funds():
        print(f"主管查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")

    print("\n=== 员工视角 ===")
    gateway.authenticate("staff")
    for fund in gateway.get_funds():
        print(f"员工查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")
