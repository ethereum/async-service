def test_import_and_version():
    import async_service

    assert isinstance(async_service.__version__, str)
