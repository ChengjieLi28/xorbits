FROM test_xorbits:test

COPY entrypoint.sh /srv/entrypoint.sh

RUN pip install pytest pytest-cov pytest-timeout pytest-forked pytest-asyncio coverage && \
    chmod a+x /srv/*.sh && \
    echo "import coverage; coverage.process_startup()" > \
    $(/opt/conda/bin/python -c "import site; print(site.getsitepackages()[-1])")/coverage.pth

ENTRYPOINT [ "/srv/entrypoint.sh" ]
