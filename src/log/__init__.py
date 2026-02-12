#!/usr/bin/env python
#  -*- coding: utf-8 -*-
"""
@ProjectName: HomeQuant
@FileName   : __init__.py
@Date       : 2026/2/12 15:45
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 日志系统
"""
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# 自定义日志级别常量
# PANIC: 最高级别，表示系统崩溃级别的错误
PANIC = 60
logging.addLevelName(PANIC, "PANIC")
# FATAL: 等同于 CRITICAL，表示致命错误
logging.addLevelName(logging.CRITICAL, "FATAL")


class Logger(logging.Logger):
    """
    自定义 Logger 类，扩展了标准 logging.Logger 功能。
    
    主要特性：
    - 支持将任意 kwargs 参数自动转换为 extra 字段
    - 新增 panic 和 fatal 日志级别
    - 保持与标准 Logger 的兼容性
    """

    def _log_with_extra(self, level: int, msg: Any, *args: Any, **kwargs: Any) -> None:
        """
        通用的日志记录方法，处理 extra 字段转换。
        
        Args:
            level: 日志级别
            msg: 日志消息
            *args: 位置参数
            **kwargs: 关键字参数
        """
        if self.isEnabledFor(level):
            kwargs = self.kwargs_to_extra(**kwargs)
            self._log(level, msg, args, **kwargs)

    def debug(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        """记录 DEBUG 级别日志。"""
        self._log_with_extra(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        """记录 INFO 级别日志。"""
        self._log_with_extra(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        """记录 WARNING 级别日志。"""
        self._log_with_extra(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        """记录 ERROR 级别日志。"""
        self._log_with_extra(logging.ERROR, msg, *args, **kwargs)

    def fatal(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        """记录 FATAL (CRITICAL) 级别日志。"""
        self._log_with_extra(logging.FATAL, msg, *args, **kwargs)

    critical = fatal

    def panic(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        """记录 PANIC 级别日志（最高级别）。"""
        self._log_with_extra(PANIC, msg, *args, **kwargs)

    def log(self, level: int, msg: Any, *args: Any, **kwargs: Any) -> None:
        """
        记录指定级别的日志。
        
        Args:
            level: 日志级别（必须是整数）
            msg: 日志消息
            *args: 位置参数
            **kwargs: 关键字参数
            
        Raises:
            TypeError: 当 level 不是整数且 logging.raiseExceptions 为 True 时
        """
        if not isinstance(level, int):
            if logging.raiseExceptions:
                raise TypeError("level must be an integer")
            else:
                return
        self._log_with_extra(level, msg, *args, **kwargs)

    @staticmethod
    def kwargs_to_extra(**kwargs: Any) -> Dict[str, Any]:
        """
        将 kwargs 中除保留字段外的参数转换为 extra 字段。
        
        保留字段：exc_info, extra, stack_info, stack_level
        其他所有参数都会被移到 extra["extra"] 字典中。
        
        Args:
            **kwargs: 关键字参数
            
        Returns:
            处理后的 kwargs 字典，包含 extra 字段
        """
        # 复制原始 kwargs 以避免副作用
        kwargs_copy = kwargs.copy()
        extra = kwargs_copy.get("extra", {}).copy()
        
        # 保留的标准日志参数
        reserved_keys = {"exc_info", "extra", "stack_info", "stacklevel"}
        
        # 将非保留参数移到 extra 中
        for key in set(kwargs_copy.keys()) - reserved_keys:
            extra[key] = kwargs_copy.pop(key)
        
        kwargs_copy["extra"] = {"extra": extra}
        return kwargs_copy


logging.setLoggerClass(klass=Logger)

# ---------------------------------------------------------------------------
#   JSONFormatter
# ---------------------------------------------------------------------------
# 有效的日志属性映射表：自定义属性名 -> LogRecord 内置属性名
_VALID_ATTRS_to_BUILTIN_ATTRS: Dict[str, str] = {
    'created': "created",
    'file_name': 'filename',
    'func_name': 'funcName',
    'level_no': 'levelno',
    'line_no': 'lineno',
    'module': 'module',
    'msecs': 'msecs',
    'path_name': 'pathname',
    'process': 'process',
    'process_name': 'processName',
    'relative_created': 'relativeCreated',
    'thread': 'thread',
    'thread_name': 'threadName'
}

_VALID_ATTRS: Set[str] = set(_VALID_ATTRS_to_BUILTIN_ATTRS.keys())


class JSONFormatter(logging.Formatter):
    """
    JSON 格式的日志格式化器。
    
    将日志记录格式化为 JSON 字符串，便于日志收集和分析。
    支持自定义包含的日志属性字段。
    """

    def __init__(self, log_keys: Optional[List[str]] = None) -> None:
        """
        初始化 JSON 格式化器。
        
        Args:
            log_keys: 需要包含在日志中的额外属性列表，
                     只有在 _VALID_ATTRS 中定义的属性才会被包含
        """
        super().__init__()
        self.log_keys: Set[str] = (set(log_keys) & _VALID_ATTRS) if log_keys else set()

    def format(self, record: logging.LogRecord) -> str:
        """
        格式化日志记录为 JSON 字符串。
        
        Args:
            record: 日志记录对象
            
        Returns:
            JSON 格式的日志字符串
        """
        extra = self.extra_from_record(record)
        extra['msg'] = record.getMessage()
        extra['time'] = datetime.fromtimestamp(record.created).astimezone().isoformat()
        extra['level'] = self.get_level_name(record.levelno).lower()
        extra['name'] = record.name
        
        # 添加用户指定的额外字段
        for key in self.log_keys:
            extra[key] = getattr(record, _VALID_ATTRS_to_BUILTIN_ATTRS[key])
        
        # 添加异常信息
        if record.exc_info:
            extra['exc_info'] = self.formatException(record.exc_info)
        
        # 添加堆栈信息
        if record.stack_info:
            extra['stack_info'] = record.stack_info
        
        return self.to_json(extra)

    @staticmethod
    def get_level_name(level_no: Union[int, str]) -> str:
        """
        获取日志级别名称。
        
        Args:
            level_no: 日志级别编号或名称
            
        Returns:
            日志级别名称字符串
        """
        if isinstance(level_no, str):
            return level_no
        
        # 标准化级别编号（向下取整到最近的 10 的倍数）
        level_no = level_no // 10 * 10
        
        # 限制在有效范围内 [0, 60]
        if level_no < 0:
            level_no = 0
        elif level_no > 60:
            level_no = 60
        
        return logging.getLevelName(level_no)

    @staticmethod
    def extra_from_record(record: logging.LogRecord) -> Dict[str, Any]:
        """
        从日志记录中提取 extra 字段。
        
        Args:
            record: 日志记录对象
            
        Returns:
            extra 字典，如果不存在则返回空字典
        """
        return record.__dict__.get("extra", {})

    @staticmethod
    def to_json(record: Dict[str, Any]) -> str:
        """
        将字典转换为 JSON 字符串。
        
        Args:
            record: 要序列化的字典
            
        Returns:
            JSON 格式的字符串
        """
        try:
            return json.dumps(record, ensure_ascii=False)
        except TypeError:
            # 如果标准序列化失败，使用自定义序列化函数
            return json.dumps(record, default=_json_serializable, ensure_ascii=False)


def _json_serializable(obj: Any) -> Union[Dict[str, Any], str]:
    """
    将不可 JSON 序列化的对象转换为可序列化格式。
    
    Args:
        obj: 需要序列化的对象
        
    Returns:
        对象的 __dict__ 属性或字符串表示
    """
    try:
        return obj.__dict__
    except AttributeError:
        return str(obj)


class LoggerAdapter(logging.LoggerAdapter):
    """
    日志适配器，用于为日志添加上下文信息。
    
    允许在不修改原始 Logger 的情况下，为所有日志记录添加额外的上下文字段。
    支持链式调用 bind 方法来累积上下文信息。
    """

    def __init__(self, logger: Logger, **kwargs: Any) -> None:
        """
        初始化日志适配器。
        
        Args:
            logger: 要包装的 Logger 实例
            **kwargs: 要添加到所有日志记录的上下文字段
        """
        self.logger = logger
        self.extra: Dict[str, Any] = kwargs

    def process(self, msg: Any, kwargs: Dict[str, Any]) -> Tuple[Any, Dict[str, Any]]:
        """
        处理日志消息和参数，合并上下文信息。
        
        Args:
            msg: 日志消息
            kwargs: 日志参数
            
        Returns:
            处理后的消息和参数元组
        """
        if "extra" in kwargs:
            extra = self.extra.copy()
            extra.update(kwargs["extra"])
            kwargs["extra"] = extra
        else:
            kwargs["extra"] = self.extra
        return msg, kwargs

    def bind(self, **kwargs: Any) -> "LoggerAdapter":
        """
        创建新的适配器实例，添加额外的上下文字段。
        
        Args:
            **kwargs: 要添加的新上下文字段
            
        Returns:
            包含合并后上下文的新 LoggerAdapter 实例
        """
        extra = self.extra.copy()
        extra.update(kwargs)
        return LoggerAdapter(self.logger, **extra)


# 设置自定义 Logger 类为默认类
logging.setLoggerClass(klass=Logger)

# 初始化根 Logger
root: Logger = Logger("root", logging.WARNING)
# logging.Logger.root 在标准库中的类型定义是 RootLogger
# 自定义 Logger 类继承自 logging.Logger，功能上完全兼容
# 使用 # type: ignore[assignment] 精确地忽略赋值类型警告，不影响其他类型检查
logging.Logger.root = root  # type: ignore[assignment]
logging.Logger.manager = logging.Manager(logging.Logger.root)


def get_logger(name: Optional[str] = None) -> Logger:
    """
    获取 Logger 实例。
    
    Args:
        name: Logger 名称，如果为 None 则返回根 Logger
        
    Returns:
        Logger 实例
        
    Examples:
        logger = get_logger("my_module")
        logger.info("This is an info message")
        root_logger = get_logger()
        root_logger.warning("This is a warning")
    """
    if name:
        return logging.Logger.manager.getLogger(name)
    else:
        return root


# 替换标准库的 getLogger 函数
logging.getLogger = get_logger
