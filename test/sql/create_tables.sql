drop schema if exists public cascade;
create schema public;

drop table if exists public.tenants cascade;
create table tenants (
    id uuid primary key,
    name text not null,
    provider text not null check (provider in ('microsoft', 'google'))
);

drop table if exists public.emails cascade;
create table emails (
    id uuid primary key default gen_random_uuid(),
    tenant_id uuid not null,
    user_id uuid not null,
    message_id text not null,
    from_address text not null,
    to_addresses text[] not null,
    subject text not null,
    body text not null,
    headers jsonb not null default '{}'::jsonb,
    received_at timestamp not null,
    provider varchar(50) not null,
    created_at timestamp not null default now(),
    unique(tenant_id, message_id)
);

create index idx_emails_tenant_user on emails(tenant_id, user_id);
create index idx_emails_received_at on emails(received_at);
create index idx_emails_message_id on emails(message_id);

drop table if exists public.ingestion_cursors cascade;
create table ingestion_cursors (
    tenant_id uuid not null,
    provider varchar(50) not null,
    user_id uuid not null,
    last_received_at timestamp not null,
    updated_at timestamp not null,
    primary key (tenant_id, provider, user_id)
);