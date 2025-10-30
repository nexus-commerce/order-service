CREATE TYPE order_status AS ENUM ('PENDING', 'PAID', 'CONFIRMED', 'CANCELLED');

CREATE TABLE IF NOT EXISTS orders (
                                      id BIGSERIAL PRIMARY KEY,
                                      user_id BIGINT NOT NULL,
                                      status order_status NOT NULL DEFAULT 'PENDING',
                                      total_price NUMERIC(10, 2) NOT NULL,
    shipping_address TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

CREATE TABLE IF NOT EXISTS order_items (
                                           id BIGSERIAL PRIMARY KEY,
                                           order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    quantity INT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    sku VARCHAR(255)
    );

INSERT INTO orders (user_id, status, total_price, shipping_address, created_at)
VALUES (1, 'CONFIRMED', 150.50, '123 Test Street, Test City', NOW());

INSERT INTO order_items (order_id, quantity, price, sku)
VALUES (1, 2, 50.25, 'SKU-TSHIRT-RED-L'),
       (1, 1, 50.00, 'SKU-JEANS-BLUE-32');