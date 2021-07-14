import * as fs from "fs";
import * as mysql from "mysql2";
import * as os from "os";
import * as path from "path";

import filenamify from "filenamify";
import sortKeys from "sort-keys";

type Config = {
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  dataPath?: string;
  tables: string[];
};

const config: Config = JSON.parse(
  fs.readFileSync(process.argv[2] ?? "config.json", "utf8")
);

type Rows = mysql.RowDataPacket[];
type Result = {
  rows: Rows;
  fields: mysql.FieldPacket[];
};

type TableRow = {
  [name: string]: string | boolean | number;
};

type TableData = {
  primaryKeys: string[];
  rows: TableRow[];
};

function query(sql: string): Promise<Result> {
  const connection = mysql.createConnection({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
  });
  return new Promise<Result>((resolve, reject) => {
    connection.query<Rows>(sql, (error, rows, fields) => {
      try {
        return error ? reject(error) : resolve({ rows, fields });
      } finally {
        connection.destroy();
      }
    });
  });
}

async function loadTable(tableName: string): Promise<TableData> {
  const { rows, fields } = await query(`SELECT * FROM ${tableName}`);
  return {
    primaryKeys: fields
      .filter((f) => (f.flags & 2) !== 0)
      .map((f) => f.name)
      .sort((a, b) => a.localeCompare(b)),
    rows: [...rows.map((row) => sortKeys({ ...row }))],
  };
}

function getKeySpec({
  primaryKeys,
  row,
}: {
  primaryKeys: string[];
  row: TableRow;
}): string {
  return primaryKeys.map((key) => `${key}=${row[key]}`).join(";");
}

async function captureTable(tableName: string): Promise<void> {
  const dataPath = (config.dataPath ?? "data").replace(
    /\$({HOME}|HOME)/g,
    os.homedir()
  );
  const tablePath = path.join(dataPath, tableName);
  if (!fs.existsSync(tablePath)) {
    fs.mkdirSync(tablePath, { recursive: true });
  }
  const oldFiles = new Set(
    fs.readdirSync(tablePath).filter((file) => path.extname(file) === ".json")
  );
  const { rows, primaryKeys } = await loadTable(tableName);
  console.info(`* ${tableName} .. ${rows.length}`);
  for (const row of rows) {
    const fileName = filenamify(getKeySpec({ primaryKeys, row })) + ".json";
    oldFiles.delete(fileName);
    fs.writeFileSync(
      path.join(tablePath, fileName),
      JSON.stringify(row, null, 2),
      "utf8"
    );
  }
  for (const oldFile of oldFiles) {
    fs.unlinkSync(path.join(tablePath, oldFile));
  }
}

async function main() {
  for (const tableName of config.tables) {
    await captureTable(tableName);
  }
}

if (require.main === module) {
  main().catch(console.error);
}
