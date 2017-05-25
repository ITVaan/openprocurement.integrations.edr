from pyramid.view import view_config


@view_config(route_name='health', renderer='json', request_method='GET')
def health(request):
    return ''
