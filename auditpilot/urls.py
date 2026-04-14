from django.urls import path

from . import views

app_name = 'auditpilot'

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('runs/upload/', views.upload_run, name='upload_run'),
    path('runs/<int:run_id>/', views.run_detail, name='run_detail'),
    path('runs/<int:run_id>/export/', views.export_run_pack, name='export_run_pack'),
    path('runs/<int:run_id>/visual/', views.visual_summary, name='visual_summary'),
    path('runs/<int:run_id>/visual.png', views.visual_summary_png, name='visual_summary_png'),
    path('runs/<int:run_id>/visual/<str:entity>/', views.visual_entity, name='visual_entity'),
    path('runs/<int:run_id>/visual/<str:entity>.png', views.visual_entity_png, name='visual_entity_png'),
    path('exceptions/', views.exception_list, name='exception_list'),
    path('exceptions/<uuid:exception_id>/', views.exception_detail, name='exception_detail'),
    path('controls/', views.control_catalog, name='control_catalog'),
    path('controls/<int:control_id>/edit/', views.control_edit, name='control_edit'),
]
