from django import template

register = template.Library()

@register.simple_tag
def describe_hit(hit):
    return hit.keys()

@register.simple_tag
def hit_source(hit):
    return hit['_source']

@register.inclusion_tag("hanalytics/_es_hit.html")
def show_hit(hit):
    return {
        "id": hit['_id'],
        "type": hit['_type'],
        "source": hit['_source'],
    }