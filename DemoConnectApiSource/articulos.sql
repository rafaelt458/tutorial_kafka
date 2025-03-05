-- Crear la tabla 'articulos' (si no la has creado aún)
CREATE TABLE IF NOT EXISTS articulos (
    codigo SERIAL PRIMARY KEY,
    descripcion VARCHAR(50) NOT NULL,
    precio NUMERIC(10, 2) NOT NULL
);

-- Insertar 200 registros aleatorios
DO $$
BEGIN
    FOR i IN 1..200 LOOP
        INSERT INTO articulos (descripcion, precio)
        VALUES (
            substring(md5(random()::text), 1, 50),
            round(cast(random() * 1000 as numeric), 2)
        );
    END LOOP;
END $$;

ALTER TABLE public.articulos OWNER TO laboratorio;
