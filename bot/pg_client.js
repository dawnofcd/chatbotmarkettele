const { Pool } = require('pg');

function isSafeIdentifier(value) {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);
}

function quoteIdentifier(identifier) {
  if (!isSafeIdentifier(identifier)) {
    throw new Error(`Invalid SQL identifier: ${identifier}`);
  }
  return `"${identifier}"`;
}

function parseColumns(columns) {
  if (!columns || columns === '*') {
    return '*';
  }

  const parts = String(columns)
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean);

  if (!parts.length) {
    return '*';
  }

  return parts.map((p) => quoteIdentifier(p)).join(', ');
}

class QueryBuilder {
  constructor(pool, table) {
    this.pool = pool;
    this.table = table;
    this.action = null;
    this.selectColumns = '*';
    this.hasSelect = false;
    this.selectOptions = {};
    this.payload = null;
    this.whereClauses = [];
    this.whereValues = [];
    this.orderBy = [];
    this.limitValue = null;
    this.offsetValue = null;
    this.expectSingle = false;
    this.expectMaybeSingle = false;
  }

  select(columns = '*', options = {}) {
    this.selectColumns = columns;
    this.hasSelect = true;
    this.selectOptions = options || {};
    if (!this.action) {
      this.action = 'select';
    }
    return this;
  }

  insert(values) {
    this.action = 'insert';
    this.payload = values;
    return this;
  }

  update(values) {
    this.action = 'update';
    this.payload = values;
    return this;
  }

  delete() {
    this.action = 'delete';
    return this;
  }

  eq(column, value) {
    const col = quoteIdentifier(column);
    if (value === null) {
      this.whereClauses.push(`${col} IS NULL`);
      return this;
    }
    this.whereValues.push(value);
    this.whereClauses.push(`${col} = $${this.whereValues.length}`);
    return this;
  }

  not(column, operator, value) {
    const col = quoteIdentifier(column);
    const op = String(operator || '').toLowerCase();
    if (op === 'is' && value === null) {
      this.whereClauses.push(`${col} IS NOT NULL`);
      return this;
    }

    if (op === 'eq') {
      this.whereValues.push(value);
      this.whereClauses.push(`${col} <> $${this.whereValues.length}`);
      return this;
    }

    throw new Error(`Unsupported not() operator: ${operator}`);
  }

  in(column, values) {
    const col = quoteIdentifier(column);
    const list = Array.isArray(values) ? values : [];
    if (!list.length) {
      this.whereClauses.push('1 = 0');
      return this;
    }

    const placeholders = list.map((v) => {
      this.whereValues.push(v);
      return `$${this.whereValues.length}`;
    });

    this.whereClauses.push(`${col} IN (${placeholders.join(', ')})`);
    return this;
  }

  is(column, value) {
    const col = quoteIdentifier(column);
    if (value === null) {
      this.whereClauses.push(`${col} IS NULL`);
    } else {
      this.whereValues.push(value);
      this.whereClauses.push(`${col} IS NOT DISTINCT FROM $${this.whereValues.length}`);
    }
    return this;
  }

  order(column, opts = {}) {
    const col = quoteIdentifier(column);
    const direction = opts.ascending === false ? 'DESC' : 'ASC';
    this.orderBy.push(`${col} ${direction}`);
    return this;
  }

  limit(value) {
    this.limitValue = Number(value);
    return this;
  }

  range(from, to) {
    const start = Number(from);
    const end = Number(to);
    if (!Number.isInteger(start) || !Number.isInteger(end) || end < start || start < 0) {
      throw new Error('Invalid range() arguments');
    }
    this.offsetValue = start;
    this.limitValue = end - start + 1;
    return this;
  }

  single() {
    this.expectSingle = true;
    this.expectMaybeSingle = false;
    return this;
  }

  maybeSingle() {
    this.expectMaybeSingle = true;
    this.expectSingle = false;
    return this;
  }

  buildWhereClause() {
    if (!this.whereClauses.length) {
      return '';
    }
    return ` WHERE ${this.whereClauses.join(' AND ')}`;
  }

  buildOrderClause() {
    if (!this.orderBy.length) {
      return '';
    }
    return ` ORDER BY ${this.orderBy.join(', ')}`;
  }

  buildLimitClause() {
    if (this.limitValue == null || Number.isNaN(this.limitValue)) {
      if (this.offsetValue == null || Number.isNaN(this.offsetValue)) {
        return '';
      }
      return ` OFFSET ${Math.max(0, this.offsetValue)}`;
    }
    let sql = ` LIMIT ${Math.max(0, this.limitValue)}`;
    if (this.offsetValue != null && !Number.isNaN(this.offsetValue)) {
      sql += ` OFFSET ${Math.max(0, this.offsetValue)}`;
    }
    return sql;
  }

  normalizeSingle(rows) {
    if (this.expectSingle) {
      if (rows.length !== 1) {
        return { data: null, error: new Error(`Expected single row, got ${rows.length}`) };
      }
      return { data: rows[0], error: null };
    }

    if (this.expectMaybeSingle) {
      if (rows.length > 1) {
        return { data: null, error: new Error(`Expected zero or one row, got ${rows.length}`) };
      }
      return { data: rows[0] || null, error: null };
    }

    return { data: rows, error: null };
  }

  async execute() {
    try {
      const table = quoteIdentifier(this.table);
      const where = this.buildWhereClause();
      const order = this.buildOrderClause();
      const limit = this.buildLimitClause();
      const values = [...this.whereValues];

      if (this.action === 'select') {
        if (this.selectOptions.head === true && this.selectOptions.count === 'exact') {
          const sql = `SELECT COUNT(*)::int AS count FROM ${table}${where}`;
          const res = await this.pool.query(sql, values);
          return { data: null, error: null, count: res.rows[0]?.count ?? 0 };
        }

        const columns = parseColumns(this.selectColumns);
        const sql = `SELECT ${columns} FROM ${table}${where}${order}${limit}`;
        const res = await this.pool.query(sql, values);
        const normalized = this.normalizeSingle(res.rows);
        return { ...normalized, count: null };
      }

      if (this.action === 'insert') {
        const rows = Array.isArray(this.payload) ? this.payload : [this.payload];
        if (!rows.length || !rows[0] || typeof rows[0] !== 'object') {
          throw new Error('Insert payload must be an object or array of objects');
        }

        const keys = Object.keys(rows[0]);
        if (!keys.length) {
          throw new Error('Insert payload has no fields');
        }

        const quotedCols = keys.map((k) => quoteIdentifier(k)).join(', ');
        const valueGroups = [];
        const insertValues = [];

        for (const row of rows) {
          const placeholders = keys.map((k) => {
            insertValues.push(row[k]);
            return `$${insertValues.length}`;
          });
          valueGroups.push(`(${placeholders.join(', ')})`);
        }

        let sql = `INSERT INTO ${table} (${quotedCols}) VALUES ${valueGroups.join(', ')}`;
        if (this.hasSelect) {
          sql += ` RETURNING ${parseColumns(this.selectColumns)}`;
        }

        const res = await this.pool.query(sql, insertValues);
        if (!this.hasSelect) {
          return { data: null, error: null, count: null };
        }
        const normalized = this.normalizeSingle(res.rows);
        return { ...normalized, count: null };
      }

      if (this.action === 'update') {
        if (!this.payload || typeof this.payload !== 'object') {
          throw new Error('Update payload must be an object');
        }

        const keys = Object.keys(this.payload);
        if (!keys.length) {
          throw new Error('Update payload has no fields');
        }

        const setParts = keys.map((k) => {
          values.push(this.payload[k]);
          return `${quoteIdentifier(k)} = $${values.length}`;
        });

        // WHERE params were built before update payload params.
        // We need to place set params first in SQL, then where params.
        const setValues = keys.map((k) => this.payload[k]);
        const whereValues = [...this.whereValues];
        const finalValues = [...setValues, ...whereValues];

        let shiftedWhere = where;
        // Shift placeholders from right to left to avoid cascading replacements:
        // "$1,$2,$3" + offset 1 -> "$2,$3,$4" (not "$4,$4,$4").
        for (let i = whereValues.length; i >= 1; i -= 1) {
          const from = new RegExp(`\\$${i}\\b`, 'g');
          shiftedWhere = shiftedWhere.replace(from, `$${i + setValues.length}`);
        }

        let sql = `UPDATE ${table} SET ${setParts.map((p, idx) => p.replace(/\$\d+/, `$${idx + 1}`)).join(', ')}${shiftedWhere}`;
        if (this.hasSelect) {
          sql += ` RETURNING ${parseColumns(this.selectColumns)}`;
        }

        const res = await this.pool.query(sql, finalValues);
        if (!this.hasSelect) {
          return { data: null, error: null, count: null };
        }
        const normalized = this.normalizeSingle(res.rows);
        return { ...normalized, count: null };
      }

      if (this.action === 'delete') {
        let sql = `DELETE FROM ${table}${where}`;
        if (this.hasSelect) {
          sql += ` RETURNING ${parseColumns(this.selectColumns)}`;
        }
        const res = await this.pool.query(sql, values);
        if (!this.hasSelect) {
          return { data: null, error: null, count: null };
        }
        const normalized = this.normalizeSingle(res.rows);
        return { ...normalized, count: null };
      }

      throw new Error('No query action specified');
    } catch (error) {
      return { data: null, error, count: null };
    }
  }

  then(resolve, reject) {
    return this.execute().then(resolve, reject);
  }

  catch(reject) {
    return this.execute().catch(reject);
  }

  finally(onFinally) {
    return this.execute().finally(onFinally);
  }
}

function createClient(config) {
  const pool = new Pool(config);
  return {
    async query(sql, params = []) {
      try {
        const text = String(sql || '').trim();
        if (!text) {
          throw new Error('SQL query is empty');
        }

        const values = Array.isArray(params) ? params : [];
        const res = await pool.query(text, values);
        return {
          data: res.rows || [],
          error: null,
          count: Number.isInteger(res.rowCount) ? res.rowCount : null,
        };
      } catch (error) {
        return { data: null, error, count: null };
      }
    },
    from(table) {
      return new QueryBuilder(pool, table);
    },
    async end() {
      await pool.end();
    },
  };
}

module.exports = { createClient };
