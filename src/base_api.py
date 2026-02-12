#!/usr/bin/env python
#  -*- coding: utf-8 -*-
"""
@ProjectName: HomeQuant
@FileName   : base_api.py
@Date       : 2026/2/12 00:35
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 基类，处理 EventLoop 相关的调用
"""
import asyncio
import functools
import sys
import time
from asyncio import Future
from typing import Optional, Coroutine


class BaseApi(object):
    """
    基类

    使用场景：
    1. 作为量化交易API的基类
    class HomeQuantApi(HomeQuantBaseApi):
        def __init__(self):
            super().__init__()
            # 初始化交易相关功能
            ...

    2. 回测引擎
    支持行情和交易的 lockstep 同步。
    通过 _run_until_idle 精确控制执行顺序
    3. 异步任务管理
    创建和管理多个异步任务
    task1 = api._create_task(fetch_quotes())
    task2 = api._create_task(process_orders())

    关键设计特点
    1. Lockstep 同步支持
    通过 monkey patch call_soon 方法，追踪事件循环状态：
    self._loop.call_soon = functools.partial(self._call_soon, self._loop.call_soon)
    用途：
    - 回测时需要行情和交易严格同步
    - 确保事件按照预期顺序执行

    2. 等待空闲机制
    等待事件循环空闲
    async def _wait_until_idle(self):
        f = Future()
        self._wait_idle_list.append(f)
        self._loop.stop()
        await f
    用途：
    - 网络连接在空闲时才接收数据
    - 保证同步代码优先于异步代码执行

    3. 超时控制
    def _run_until_task_done(self, task, deadline=None):
    if deadline is not None:
        deadline_handle = self._loop.call_later(
            max(0, deadline - time.time()),
            self._set_wait_timeout
        )
    用途：
    - 防止任务无限等待
    - 支持超时后的错误处理

    典型使用流程
    1. 创建API实例
    api = HomeQuantBaseApi()

    2. 创建异步任务
    task = api._create_task(async_function())

    3. 运行到任务完成
    api._run_until_task_done(task, deadline=time.time() + 10)

    4. 清理资源
    api._close_tasks()
    api._close_loop()

    与其他模块的关系
    可能的继承关系
    HomeQuantBaseApi (BaseApi.py)
    ↓
    HomeQuantApi (量化主API)
        ↓
    用户策略代码
    配合使用的模块
    网络连接模块：使用 _wait_until_idle 控制数据接收
    回测引擎：使用 _run_until_idle 实现 lockstep
    交易模块：使用 _create_task 管理异步订单

    总结
    BaseApi.py 是一个高级异步事件循环管理基类，主要用于：
    - 量化交易框架 - 作为交易API的基础类
    - 回测引擎 - 提供精确的执行控制
    - 异步任务管理 - 统一管理所有异步任务
    - 异常处理 - 收集和处理任务异常
    - 跨平台兼容 - 处理Windows平台特殊问题
    这个类的设计非常精巧，特别是 lockstep 同步机制和等待空闲机制，是实现高质量回测和实盘交易的关键基础设施。
    """
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """
        创建接口实例

        Args:
            loop(asyncio.AbstractEventLoop): [可选] 使用指定的 IOLoop, 默认创建一个新的.
        """
        self._loop = asyncio.SelectorEventLoop() if loop is None else loop  # 创建一个新的 ioloop, 避免和其他框架/环境产生干扰
        self._event_rev, self._check_rev = 0, 0
        self._wait_idle_list = []  # 所有等待 loop idle 的 Future
        self._wait_timeout = False  # wait_update 是否触发超时
        self._tasks = set()  # 由api维护的所有根task，不包含子task，子task由其父task维护
        self._exceptions = []  # 由api维护的所有task抛出的例外
        # 回测需要行情和交易 lockstep, 而 asyncio 没有将内部的 _ready 队列暴露出来,
        # 因此 monkey patch call_soon 函数用来判断是否有任务等待执行
        self._loop.call_soon = functools.partial(self._call_soon, self._loop.call_soon)
        # Windows系统下asyncio不支持KeyboardInterrupt的临时补丁
        if sys.platform.startswith("win"):
            self._create_task(self._windows_patch())

    def _create_task(self, coro: Coroutine, _caller_api: bool = False) -> asyncio.Task:
        task = self._loop.create_task(coro)
        current_task = asyncio.current_task(loop=self._loop)
        if current_task is None or _caller_api:  # 由 api 创建的 task，需要 api 主动管理
            self._tasks.add(task)
            task.add_done_callback(self._on_task_done)
        return task

    async def _cancel_tasks(self, *tasks):
        # 目前的 task 退出流程无法处理在 finally 中被 cancel 的情况,
        # 例如: twap _run 中调用 _insert_order 并且刚好时间段结束执行到 finally 时整个 api 被 close 触发 CancelError
        if len(tasks) == 0:
            return
        task = tasks[0]
        other_tasks = tasks[1:]
        try:
            await self._cancel_task(task)
        finally:
            if tasks:
                await self._cancel_tasks(*other_tasks)

    @staticmethod
    async def _cancel_task(task):
        exception = None
        task.cancel()
        # 如果 task 已经 done，可以调用 cancel 但是 cancelled 不会变成 True
        while not task.done():
            try:
                await asyncio.shield(task)  # task 不会再被 cancel
            except asyncio.CancelledError as ex:
                if not task.cancelled():
                    exception = ex
        await asyncio.sleep(0)  # Give callbacks a chance to run
        if exception:
            raise exception from None

    def _call_soon(self, org_call_soon, callback, *args, **kwargs):
        """ioloop.call_soon的补丁, 用来追踪是否有任务完成并等待执行"""
        self._event_rev += 1
        return org_call_soon(callback, *args, **kwargs)

    def _run_once(self):
        """执行 ioloop 直到 ioloop.stop 被调用"""
        if not self._exceptions:
            self._loop.run_forever()
        if self._exceptions:
            raise self._exceptions.pop(0)

    def _run_until_idle(self, async_run=False):
        """执行 ioloop 直到没有待执行任务
        async_run is True 会从 _wait_idle_list 中取出等待的异步任务，保证同步代码优先于异步代码执行，
        只有被 _run_until_task_done 调用（即 api 等待 fetch_msg）时，async_run 会为 True
        """
        while self._check_rev != self._event_rev:
            check_handle = self._loop.call_soon(self._check_event, self._event_rev + 1)
            try:
                self._run_once()
            finally:
                check_handle.cancel()
        if len(self._wait_idle_list) > 0 and async_run:
            f = self._wait_idle_list.pop(0)  # 取出 list 中的第一个 Future
            f.set_result(None)  # f 返回

    async def _wait_until_idle(self):
        """等待 ioloop 执行到空闲时，才从网络连接处收数据包，在 TqConnect 类中使用"""
        f = Future()
        self._wait_idle_list.append(f)
        self._loop.stop()
        await f

    def _run_until_task_done(self, task: asyncio.Task, deadline: Optional[float] = None) -> None:
        deadline_handle = None
        try:
            self._wait_timeout = False
            if deadline is not None:
                deadline_handle = self._loop.call_later(max(0.0, deadline - time.time()), self._set_wait_timeout)
            while not self._wait_timeout and not task.done():
                if len(self._wait_idle_list) == 0:
                    self._run_once()
                else:
                    self._run_until_idle(async_run=True)
        finally:
            if deadline is not None:  # 如果有 deadline 则取消定时器
                deadline_handle.cancel()
            task.cancel()

    def _check_event(self, rev):
        self._check_rev = rev
        self._loop.stop()

    def _set_wait_timeout(self):
        self._wait_timeout = True
        self._loop.stop()

    def _on_task_done(self, task):
        """当由 api 维护的 task 执行完成后取出运行中遇到的例外并停止 ioloop"""
        try:
            exception = task.exception()
            if exception:
                self._exceptions.append(exception)
        except asyncio.CancelledError:
            pass
        finally:
            self._tasks.remove(task)
            self._loop.stop()

    @staticmethod
    async def _windows_patch():
        """Windows系统下asyncio不支持KeyboardInterrupt的临时补丁, 详见 https://bugs.python.org/issue23057"""
        while True:
            await asyncio.sleep(1)

    def _close_tasks(self) -> None:
        self._run_until_idle(async_run=False)  # 由于有的处于 ready 状态 task 可能需要报撤单, 因此一直运行到没有 ready 状态的 task
        for task in self._tasks:
            task.cancel()
        while self._tasks:  # 等待 task 执行完成
            self._run_once()

    def _close_loop(self) -> None:
        self._loop.run_until_complete(self._loop.shutdown_asyncgens())
        self._loop.close()
