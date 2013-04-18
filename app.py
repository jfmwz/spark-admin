from handlers import *
from tornado.options import define, options
import os
import tornado.httpserver
import tornado.web

DEBUG = False
PROJECT_PATH = os.path.abspath(os.path.dirname(__file__))
define("port", default=8888, help="run on the given port", type=int)
define("debug", default=DEBUG, type=bool)


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r'/', HomeHandler),
            (r'/new_cluster', NewClusterHandler),
            (r'/sql_console', SqlConsoleHandler),
            (r'/cluster/(.*)', ClusterHandler),
            (r'/action', ActionHandler),
            (r'/settings', SettingsHandler),
            (r'/about', AboutHandler),
        ]

        settings = {
            'template_path': os.path.join(PROJECT_PATH, 'templates'),
            'static_path': os.path.join(PROJECT_PATH, 'static'),
            'debug': True,
        }
        tornado.web.Application.__init__(self, handlers, **settings)


if __name__ == '__main__':
    if not DEBUG:
        # Redirect to log file
        so = se = open("static/server.log", 'w', 0)
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

    tornado.options.parse_command_line()
    print 'Running server on port %s...' % options.port
    
#     log = open('static/server.log', 'a+')
#     ctx = daemon.DaemonContext(
#             stdout=log,
#             stderr=log,
#             working_directory='.',
#             pidfile=lockfile.FileLock('pidfile', threaded=False)
#     )
#     ctx.open()    
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
