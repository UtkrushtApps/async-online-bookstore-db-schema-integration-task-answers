# Solution Steps

1. Create the PostgreSQL schema in 'db/schema.sql' with tables for authors, categories, and books.

2. Authors table: Define 'id' (serial primary key), 'name' (unique, non-null).

3. Categories table: Define 'id' (serial primary key), 'name' (unique, non-null).

4. Books table: Define 'id' (serial primary key), 'title' (non-null), 'author_id' (foreign key), 'category_id' (foreign key, can be null with ON DELETE SET NULL), and optional 'description' field.

5. Add foreign key constraints: Books.author_id references Authors.id (ON DELETE CASCADE), Books.category_id references Categories.id (ON DELETE SET NULL).

6. In 'db/db_utils.py', implement a Database class using asyncpg with connect/disconnect methods for PostgreSQL connection pooling.

7. Implement CRUD utility functions for Authors: create_author, get_author_by_id, list_authors.

8. Implement CRUD utility functions for Categories: create_category, get_category_by_id, list_categories.

9. Implement CRUD and query functions for Books: create_book, get_book_by_id (including joining to author and category), list_books, list_books_by_author, list_books_by_category, delete_book.

10. Ensure all methods are async, properly use asyncpg's connection pool, and return dictionaries/records suitable for FastAPI integration.

11. Test database utilities independently or integrate into FastAPI routes to allow non-blocking DB access for all required bookstore operations.

