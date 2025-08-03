import asyncpg
from typing import List, Optional, Dict

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn)

    async def disconnect(self):
        await self.pool.close()

    # ------------------------------------
    # AUTHORS CRUD
    # ------------------------------------
    async def create_author(self, name: str) -> Dict:
        query = 'INSERT INTO authors (name) VALUES ($1) RETURNING id, name'
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, name)

    async def get_author_by_id(self, author_id: int) -> Optional[Dict]:
        query = 'SELECT id, name FROM authors WHERE id=$1'
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, author_id)

    async def list_authors(self) -> List[Dict]:
        query = 'SELECT id, name FROM authors ORDER BY name'
        async with self.pool.acquire() as conn:
            return await conn.fetch(query)

    # ------------------------------------
    # CATEGORIES CRUD
    # ------------------------------------
    async def create_category(self, name: str) -> Dict:
        query = 'INSERT INTO categories (name) VALUES ($1) RETURNING id, name'
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, name)

    async def get_category_by_id(self, category_id: int) -> Optional[Dict]:
        query = 'SELECT id, name FROM categories WHERE id=$1'
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, category_id)

    async def list_categories(self) -> List[Dict]:
        query = 'SELECT id, name FROM categories ORDER BY name'
        async with self.pool.acquire() as conn:
            return await conn.fetch(query)

    # ------------------------------------
    # BOOKS CRUD & QUERIES
    # ------------------------------------
    async def create_book(self, title: str, author_id: int, category_id: int, description: Optional[str]) -> Dict:
        query = '''
            INSERT INTO books (title, author_id, category_id, description)
            VALUES ($1, $2, $3, $4)
            RETURNING id, title, author_id, category_id, description
        '''
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, title, author_id, category_id, description)

    async def get_book_by_id(self, book_id: int) -> Optional[Dict]:
        # Join with author and category for richer detail
        query = '''
            SELECT b.id, b.title, a.id as author_id, a.name as author_name, c.id as category_id, c.name as category_name, b.description
            FROM books b
            JOIN authors a ON b.author_id = a.id
            LEFT JOIN categories c ON b.category_id = c.id
            WHERE b.id=$1
        '''
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, book_id)

    async def list_books(self) -> List[Dict]:
        query = '''
            SELECT b.id, b.title, a.id as author_id, a.name as author_name, c.id as category_id, c.name as category_name, b.description
            FROM books b
            JOIN authors a ON b.author_id = a.id
            LEFT JOIN categories c ON b.category_id = c.id
            ORDER BY b.title
        '''
        async with self.pool.acquire() as conn:
            return await conn.fetch(query)

    async def list_books_by_author(self, author_id: int) -> List[Dict]:
        query = '''
            SELECT b.id, b.title, a.id as author_id, a.name as author_name, c.id as category_id, c.name as category_name, b.description
            FROM books b
            JOIN authors a ON b.author_id = a.id
            LEFT JOIN categories c ON b.category_id = c.id
            WHERE b.author_id = $1
            ORDER BY b.title
        '''
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, author_id)

    async def list_books_by_category(self, category_id: int) -> List[Dict]:
        query = '''
            SELECT b.id, b.title, a.id as author_id, a.name as author_name, c.id as category_id, c.name as category_name, b.description
            FROM books b
            JOIN authors a ON b.author_id = a.id
            LEFT JOIN categories c ON b.category_id = c.id
            WHERE b.category_id = $1
            ORDER BY b.title
        '''
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, category_id)

    async def delete_book(self, book_id: int) -> None:
        query = 'DELETE FROM books WHERE id=$1'
        async with self.pool.acquire() as conn:
            await conn.execute(query, book_id)
