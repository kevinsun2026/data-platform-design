"""Connector package — Protocol + 7 driver implementations.

The connectors expose a uniform :class:`~aidp_datasource.connectors.base.Connector`
Protocol that the datasource service consumes. The factory function
:func:`build_connector` returns the right concrete connector for a
given ``Datasource.kind`` (one of ``"postgresql"`` / ``"mysql"`` /
``"oracle"`` / ``"hive"`` / ``"mongodb"`` / ``"doris"`` /
``"kafka"``).

Drivers are lazy-imported inside the factory so an operator who
registers only Postgres datasources never pays the import cost of
``oracledb`` / ``pyhive`` / ``pymongo`` / ``pymysql`` /
``aiokafka``. Test environments can also avoid the cost of those
native extensions by stubbing the factory.
"""

from __future__ import annotations
