API
===


ABC
~~~

ManagerAPI
----------

.. autoclass:: async_service.abc.ManagerAPI
    :members:
    :undoc-members:
    :show-inheritance:


InternalManagerAPI
------------------

.. autoclass:: async_service.abc.InternalManagerAPI
    :members:
    :undoc-members:
    :show-inheritance:


ServiceAPI
----------

.. autoclass:: async_service.abc.ServiceAPI
    :members:
    :undoc-members:
    :show-inheritance:


Base
~~~~

BaseManager
-----------

.. autoclass:: async_service.base.BaseManager
    :members:
    :undoc-members:
    :show-inheritance:

Service
-------

.. autoclass:: async_service.base.Service
    :members:
    :undoc-members:
    :show-inheritance:


Asyncio
~~~~~~~

AsyncioManager
--------------

.. autoclass:: async_service.asyncio.AsyncioManager
    :members:
    :undoc-members:
    :show-inheritance:

background_asyncio_service
--------------------------

.. autofunction:: async_service.asyncio.background_asyncio_service

external_api
--------------------------

.. autofunction:: async_service.asyncio.external_api


Trio
~~~~

TrioManager
-----------

.. autoclass:: async_service.asyncio.AsyncioManager
    :members:
    :undoc-members:
    :show-inheritance:

background_trio_service
--------------------------

.. autofunction:: async_service.asyncio.background_asyncio_service


Exceptions
~~~~~~~~~~

DaemonTaskExit
--------------

.. autoclass:: async_service.exceptions.DaemonTaskExit
    :members:
    :undoc-members:
    :show-inheritance:

LifecycleError
--------------

.. autoclass:: async_service.exceptions.LifecycleError
    :members:
    :undoc-members:
    :show-inheritance:


ServiceCancelled
----------------

.. autoclass:: async_service.exceptions.ServiceCancelled
    :members:
    :undoc-members:
    :show-inheritance:


ServiceException
----------------

.. autoclass:: async_service.exceptions.ServiceException
    :members:
    :undoc-members:
    :show-inheritance:
