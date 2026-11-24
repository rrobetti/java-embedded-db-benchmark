package io.github.embeddedbench.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

final class Schema {
    private Schema() {
    }

    static void create(Connection connection, String binaryType) throws SQLException {
        String[] definitions = {
                "CREATE TABLE customers (id BIGINT PRIMARY KEY, name VARCHAR(100) NOT NULL, "
                        + "email VARCHAR(160) NOT NULL UNIQUE, notes VARCHAR(200), payload " + binaryType + " NOT NULL)",
                "CREATE TABLE accounts (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES customers(id), "
                        + "balance DECIMAL(18,2) NOT NULL)",
                "CREATE TABLE products (id BIGINT PRIMARY KEY, name VARCHAR(100) NOT NULL, price DECIMAL(18,2) NOT NULL)",
                "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES customers(id), "
                        + "created_at TIMESTAMP NOT NULL, status VARCHAR(20) NOT NULL)",
                "CREATE TABLE order_items (id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES orders(id), "
                        + "product_id BIGINT NOT NULL REFERENCES products(id), quantity INTEGER NOT NULL)",
                "CREATE TABLE financial_transactions (id BIGINT PRIMARY KEY, account_id BIGINT NOT NULL REFERENCES accounts(id), "
                        + "amount DECIMAL(18,2) NOT NULL, created_at TIMESTAMP NOT NULL)",
                "CREATE INDEX idx_accounts_customer ON accounts(customer_id)",
                "CREATE INDEX idx_orders_customer ON orders(customer_id)",
                "CREATE INDEX idx_orders_created ON orders(created_at)",
                "CREATE INDEX idx_order_items_order ON order_items(order_id)",
                "CREATE INDEX idx_order_items_product ON order_items(product_id)",
                "CREATE INDEX idx_transactions_account ON financial_transactions(account_id)",
                "CREATE INDEX idx_transactions_created ON financial_transactions(created_at)"
        };
        try (Statement statement = connection.createStatement()) {
            for (String definition : definitions) {
                statement.executeUpdate(definition);
            }
        }
    }
}
