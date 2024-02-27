# Microsoft SQL Server

## Overview

To use Microsoft SQL Server as a destination, you will need to specify the following:

1. Host
2. Port
3. Service Account username
4. Service Account password
5. Database Name

### Creating a service account

```sql
CREATE LOGIN artie_transfer WITH PASSWORD = 'AStrongPassword!';
GO

USE database;
GO

CREATE USER artie_transfer FOR LOGIN artie_transfer;
GO

GRANT ALTER ON SCHEMA::dbo TO artie_transfer;
GO

GRANT CREATE TABLE, INSERT, UPDATE, DELETE TO artie_transfer;
GO
```

### Typing

| Artie Type | Microsoft SQL Server Type                  |
| ---------- | ------------------------------------------ |
| Float      | Float                                      |
| Integer    | Big Int                                    |
| Numeric    | Numeric with precision and scale specified |
| Boolean    | Bit                                        |
| String     | VARCHAR                                    |
| Struct     | NVARCHAR                                   |
| Array      | NVARCHAR                                   |
| Timestamp  | Datetime2                                  |
| Time       | Time                                       |
| Date       | Date                                       |
