from django.shortcuts import render_to_response

def viewer(request):
    return render_to_response("hansard/viewer.html", {})