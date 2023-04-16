from django.shortcuts import render
from .query_metrics_collector import QueryMetricsCollector

def home(request):
    return render(request, 'index.html')

def home_back(request):
    return render(request, 'index.html')

def query_metrics(request):
    if request.method == 'POST':
        query = request.POST.get('query_text', '')

        query_metrics_collector = QueryMetricsCollector(db_name='term_project', user='postgres', password='dakshana', host='127.0.0.1', port='5432')
        query_metrics = query_metrics_collector.get_query_metrics(query)
        
        return render(request, 'result.html', {'query_metrics': query_metrics})
    
    return render(request, 'index.html')
