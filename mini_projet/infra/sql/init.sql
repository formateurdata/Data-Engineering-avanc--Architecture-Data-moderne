-- Exécuté par PostgreSQL une seule fois, à la création de la base.

CREATE TABLE public.customers (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(150),
    city VARCHAR(100),
    loyalty_points INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Le WAL garde toute l'ancienne ligne : l'événement d'un UPDATE ou d'un DELETE porte un « before » complet
ALTER TABLE public.customers REPLICA IDENTITY FULL;

-- Des clients présents avant le démarrage de la capture, pour que le snapshot initial ait quelque chose à lire
INSERT INTO public.customers (full_name, email, city, loyalty_points) VALUES
    ('Client Un', 'client.un@example.com', 'Paris', 120),
    ('Client Deux', 'client.deux@example.com', 'Lyon', 45),
    ('Client Trois', 'client.trois@example.com', 'Lille', 300),
    ('Client Quatre', 'client.quatre@example.com', 'Nantes', 0),
    ('Client Cinq', 'client.cinq@example.com', 'Marseille', 80);

-- Rôle de la capture : il lit la table et ouvre un flux de réplication, sans droit d'écriture
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT SELECT ON public.customers TO debezium;

-- La publication liste les tables à répliquer. Le slot, lui, est créé par Debezium à son premier démarrage :
-- créé ici, il retiendrait le WAL sans que personne ne le lise.
CREATE PUBLICATION dbz_customers FOR TABLE public.customers;
