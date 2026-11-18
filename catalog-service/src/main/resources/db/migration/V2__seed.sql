INSERT INTO products (sku, name, category, unit_price, currency, stock) VALUES
    ('SKU-001', 'Teclado mecánico',        'ELECTRONICS', 120.00, 'USD', 50),
    ('SKU-002', 'Mouse inalámbrico',       'ELECTRONICS',  45.50, 'USD', 100000),
    ('SKU-003', 'Monitor 27"',             'ELECTRONICS', 310.00, 'USD', 5),
    ('SKU-004', 'Clean Architecture',      'BOOKS',        38.90, 'USD', 20),
    ('SKU-005', 'Release It! 2nd Edition', 'BOOKS',        42.00, 'USD', 1);

-- Descuento por volumen según categoría: se aplica la regla con mayor min_quantity <= cantidad.
INSERT INTO discounts (category, min_quantity, percentage) VALUES
    ('ELECTRONICS', 3,  5.00),
    ('ELECTRONICS', 10, 12.00),
    ('BOOKS',       2,  10.00),
    ('BOOKS',       5,  20.00);
