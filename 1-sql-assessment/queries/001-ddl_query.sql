-- Stores information about retail branches
CREATE TABLE branch_details (
    branch_code VARCHAR(10) PRIMARY KEY,
    branch_name VARCHAR(100),
    region_id INT,
    operation_start_date DATE,
    is_active BOOLEAN,
    max_capacity INT,
    last_renovation_date DATE
);
-- Staff information across branches
CREATE TABLE staff_records (
    staff_id VARCHAR(15) PRIMARY KEY,
    branch_code VARCHAR(10),
    join_date DATE,
    role_code VARCHAR(5),
    base_salary DECIMAL(12,2),
    supervisor_id VARCHAR(15),
    resignation_date DATE,
    performance_score DECIMAL(3,2),
    FOREIGN KEY (branch_code) REFERENCES branch_details(branch_code)
);
-- Product catalog with hierarchical categories
CREATE TABLE product_hierarchy (
    product_code VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(100),
    category_l1 VARCHAR(50), -- Main category
    category_l2 VARCHAR(50), -- Sub-category
    category_l3 VARCHAR(50), -- Detailed category
    unit_cost DECIMAL(12,2),
    unit_price DECIMAL(12,2),
    supplier_id VARCHAR(10),
    minimum_stock INT,
    is_perishable BOOLEAN
);
-- Daily inventory movements
CREATE TABLE inventory_movements (
    movement_id BIGINT PRIMARY KEY,
    product_code VARCHAR(20),
    branch_code VARCHAR(10),
    transaction_type CHAR(3), -- IN, OUT, ADJ (adjustment), RTN (return), EXP (expired)
    quantity INT,
    transaction_date DATE,
    batch_number VARCHAR(20),
    expiry_date DATE,
    unit_cost_at_time DECIMAL(12,2),
    FOREIGN KEY (product_code) REFERENCES product_hierarchy(product_code),
    FOREIGN KEY (branch_code) REFERENCES branch_details(branch_code)
);
-- Sales transactions
CREATE TABLE sales_transactions (
    transaction_id VARCHAR(30) PRIMARY KEY,
    branch_code VARCHAR(10),
    transaction_date TIMESTAMP,
    staff_id VARCHAR(15),
    payment_method VARCHAR(10),
    total_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    loyalty_points_earned INT,
    loyalty_points_redeemed INT,
    customer_id VARCHAR(20),
    FOREIGN KEY (branch_code) REFERENCES branch_details(branch_code),
    FOREIGN KEY (staff_id) REFERENCES staff_records(staff_id)
);
-- Sales transaction details
CREATE TABLE sales_line_items (
    transaction_id VARCHAR(30),
    line_number INT,
    product_code VARCHAR(20),
    quantity INT,
    unit_price_at_time DECIMAL(12,2),
    discount_percentage DECIMAL(5,2),
    total_line_amount DECIMAL(12,2),
    PRIMARY KEY (transaction_id, line_number),
    FOREIGN KEY (transaction_id) REFERENCES sales_transactions(transaction_id),
    FOREIGN KEY (product_code) REFERENCES product_hierarchy(product_code)
);