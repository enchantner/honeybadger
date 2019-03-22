import asyncio
import concurrent.futures
import logging
import sys
import time
import uuid

import aiohttp
import uvloop

from aiohttp import web

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logging.basicConfig(**{
    "level": logging.DEBUG,
    "format": (
        "[%(asctime)s] %(levelname)s %(message)s"
    ),
    "datefmt": "%H:%M:%S",
    "stream": sys.stderr
})
logger = logging.getLogger(__name__)


app = web.Application()
app['TASKS'] = {}


async def fetch(task_id, url):
    res = {
        url: {
            "timestamp": int(time.time()),
            "errors": {}
        }
    }

    res[url]["history"] = []

    errors = {}

    # verify_ssl=verify_ssl, ssl_context=ssl_context)
    conn = aiohttp.TCPConnector()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/58.0.3029.110 Safari/537.36"
        )
    }

    async with aiohttp.ClientSession(
            headers=headers,
            connector=conn) as session:
        try:
            async with session.head(
                url,
                allow_redirects=True,
                timeout=20
            ) as resp:
                await resp.text()
                async with resp:
                    if resp.status == 404:
                        errors["http"] = "404"
                    for redirect in resp.history:
                        redirect_url = dict(
                            redirect.raw_headers
                        ).get(b"Location").decode()
                        res[url]["history"].append(redirect_url)
        except aiohttp.ClientResponseError as exc:
            errors["http"] = "RESPONSE_ERROR"
            logger.error(url, exc_info=exc)
        except aiohttp.ClientSSLError as exc:
            errors["http"] = "SSL_ERROR"
            logger.error(url, exc_info=exc)
        except aiohttp.ClientOSError as exc:
            errors["http"] = "CONNECTION_ERROR"
            logger.error(url, exc_info=exc)
        except concurrent.futures._base.TimeoutError as exc:
            errors["http"] = "TIMEOUT"
            logger.debug("timeout %s", url)
        except Exception as exc:
            errors["http"] = str(exc)
            logger.error(url, exc_info=exc)

    res[url]["errors"] = errors

    return res


async def bound_fetch(task_id, sem, url):
    # Getter function with semaphore.
    async with sem:
        return await fetch(task_id, url)


async def worker(task_id, urls):
    sem = asyncio.Semaphore(1000)

    for url in urls:
        task = asyncio.ensure_future(
            bound_fetch(task_id, sem, url)
        )
        app['TASKS'][task_id]['workers'].append(task)

    responses = await asyncio.gather(*app['TASKS'][task_id]['workers'],
                                     return_exceptions=True)

    response_dict = {}
    for r in responses:
        if isinstance(r, Exception):
            logger.error(r)
        else:
            for k, v in r.items():
                response_dict[k] = v

    app['TASKS'][task_id]["result"] = response_dict
    app['TASKS'][task_id]["status"] = "ready"
    logger.debug("Worker finished")


async def submit(request):
    data = await request.json()
    urls_count = len(data["urls"])

    task_id = str(uuid.uuid4())
    request.app['TASKS'][task_id] = {
        "status": "running",
        "workers": [],
        "result": {}
    }
    logger.info(
        "Task '{0}' created: {1} URLs received".format(
            task_id,
            urls_count
        )
    )

    asyncio.ensure_future(
        worker(task_id, data["urls"])
    )

    return web.json_response({"task_id": task_id})


async def task_status(request):
    task_id = request.match_info.get('task_id')

    if not task_id or task_id not in request.app['TASKS']:
        return web.Response(status=404)

    task = app['TASKS'].get(task_id)
    return web.json_response({
        "status": task.get("status"),
        "result": task.get("result", {})
    })


async def task_delete(request):
    task_id = request.match_info.get('task_id')

    if not task_id or task_id not in request.app['TASKS']:
        return web.Response(status=404)

    del app['TASKS'][task_id]
    return web.Response(status=204)


async def tasks(request):
    return web.json_response({
        k: {
            "status": v["status"],
            "result": v.get("result", {})
        } for k, v in request.app['TASKS'].items()
    })


def run_service(host, port):
    app.router.add_post('/submit', submit)
    app.router.add_get('/tasks', tasks)
    app.router.add_get('/tasks/{task_id}', task_status)
    app.router.add_delete('/tasks/{task_id}', task_delete)
    web.run_app(app, host=host, port=port)


if __name__ == "__main__":
    run_service("0.0.0.0", 7777)
