"""mini_fkdb 自定义异常。"""


class FKDBError(Exception):
    """所有 mini_fkdb 异常的基类。"""


class SchemaError(FKDBError):
    """建表 / 外键定义不合法。"""


class NotFound(FKDBError):
    """主键行不存在。"""


class UniqueViolation(FKDBError):
    """主键或唯一约束冲突。"""


class ForeignKeyViolation(FKDBError):
    """
    外键约束被违反。

    kind:
      - "blocked"   RESTRICT / NO ACTION 阻止了删除或更新
      - "orphan"    插入/更新子行时引用了不存在的父行
      - "deferred"  事务提交时仍未消解的 NO ACTION 冲突
    """

    def __init__(self, message, kind="orphan", details=None):
        super().__init__(message)
        self.kind = kind
        self.details = details or {}


class LockTimeout(FKDBError):
    """等待行锁超时（通常意味着死锁）。"""


class TransactionAborted(FKDBError):
    """事务已经进入失败状态，必须回滚。"""
