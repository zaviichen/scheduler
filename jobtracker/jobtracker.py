#!/usr/bin/python
"""
"""

import web
from socket_client import sendevent
import json

render = web.template.render('templates/')
web.template.Template.globals['render'] = render

config = web.storage(
    static = '/static'
)
web.template.Template.globals['config'] = config

urls = (
    '/', 'index'
)

SPAN = """
<li>
<span class="job">%(job)s</span>
<span class="%(color)s status">%(status)s</span>
<span><i class="icon-time"></i>%(runtime)s</span>
%(sub)s
</li>"""

class index:
    def GET(self):
        rsp = sendevent('runtime_infos',None)
        data = json.loads(rsp)
        jobs = data['response']
        spans = self.assemble_span(jobs)
        return render.index(spans);

    def assemble_span(self, data):
        spans = []
        for job in data:
            print job
            dep = self.get_dep(job[0])
            span_dic = {
                'job': job[0],
                'status': job[4],
                'runtime': '%s - %s' % (job[7], job[8]),
                'color': STATUS_COLOR.get(job[4].lower(), ''),
                'sub': dep
            }
            spans.append(SPAN % span_dic)
        return spans

    def get_dep(self, job):
        pamras = {'-j':job, '-l':1}
        rsp = sendevent('job_dep',pamras)
        data = json.loads(rsp)
        deps = data['response'][job]
        spans = []
        if len(deps) > 0:
            for dep in deps:
                span_dic = {
                    'job': dep,
                    'status': 'success',
                    'runtime': '',
                    'color': STATUS_COLOR.get('success', ''),
                    'sub': ''
                }
                spans.append(SPAN % span_dic)
            return '<ul>%s</ul>' % '\n'.join(spans)
        else:
            return ''


STATUS_COLOR = {
    'success': 'badge badge-success',
    'pending': 'badge badge-info',
    'fail': 'badge badge-important',
    'running': 'badge badge-warning'
}

def main():
    app = web.application(urls, globals())
    app.run()


if __name__ == '__main__':
    main()
