truncate table public.tenants cascade;
truncate table public.emails cascade;
truncate table public.ingestion_cursors cascade;

insert into public.tenants (id, name, provider) values 
    ('d290f1ee-6c54-4b01-90e6-d701748f0851', 'Voyage Privé', 'microsoft'),
    ('c4b1d2f3-3c4b-4f5a-8e9d-0a1b2c3d4e5f', 'api.video', 'google');