from aiohttp import web

async def handle(request):
    res = { "hello": "world"}
    return web.json_response(res)

if __name__ == "__main__":
  app = web.Application()
  app.add_routes([web.get('/', handle)])
  web.run_app(app)
