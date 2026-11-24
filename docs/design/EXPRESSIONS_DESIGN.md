# Expressions (engine)
Conditions (`IF`, `WHERE`) and computed assignments (`SET BLOCK ... coins = coins + $1`) share one expression engine in `src/expr.hpp`.
This note records its semantics as implemented; SET BLOCK and DELETE BLOCK use it for IF and computed values (#145), and AGGREGATE and SCAN CHUNKS for WHERE; see the last two sections.

## Grammar

```
expr     := and_expr { OR and_expr }
and_expr := not_expr { AND not_expr }
not_expr := NOT not_expr | pred
pred     := '(' expr ')' | EXISTS | sum cmp sum | sum IS [NOT] NULL
          | sum [NOT] IN '(' (value | $n) {',' (value | $n)} ')' | sum [NOT] BETWEEN sum AND sum | bool_operand
sum      := term { ('+' | '-') term }
term     := factor { ('*' | '/' | '%') factor }
factor   := '-' factor | value | $n | column | chunk.column | POS_X | POS_Y | POS_Z | NOW()
          | LEAST '(' sum {',' sum} ')' | GREATEST '(' sum {',' sum} ')' | COALESCE '(' sum {',' sum} ')' | '(' sum ')'
cmp      := '=' | '<>' | '<' | '<=' | '>' | '>='
```

An assignment is `column = sum`, and a list separates assignments with commas.
Arithmetic and comparisons do not chain without parentheses (`a < b < c` is refused); `+ -` and `* / %` associate to the left.
A predicate in parentheses cannot be an operand of arithmetic or a comparison: `(a = 1) + 1` is a syntax error.
`-` before a number literal makes a negative literal, so `-9223372036854775808` is one value; IN lists accept such literals.

## Names and values

Keywords are case-insensitive: AND, OR, NOT, IS, NULL, IN, BETWEEN, EXISTS, TRUE, FALSE, INF, NAN, NOW, LEAST, GREATEST, COALESCE, POS_X, POS_Y, POS_Z, CHUNK, and IF and WHERE, which end an expression inside a statement.
A column with one of these names is written `"name"`; any column may be quoted.
Bare and quoted names follow the column-name rule (`[a-z_][a-z0-9_]*`, at most 63 bytes).
`chunk.name` names a chunk-level column. `version` is an ordinary name.
Values are the CQL literals and `$n` parameters, numbered and used once as in statements.

Inside a statement, IF starts the version clause only when VERSION is followed by an integer (a digit, or `-` and a digit) or a parameter; anything else after IF is a condition (`IsVersionClause`).

## Types

Planning binds names through a schema interface (name to id, slot, type and nullability, separately for block and chunk columns) and types every node before evaluation.

- Integers of every width, positions (i64) and `NOW()` (u64, milliseconds supplied by the caller) compute exactly in 128 bits.
  A result outside the 128-bit range is `OUT_OF_RANGE`; `/` truncates toward zero, `%` takes the dividend's sign, and dividing by zero is `INVALID_ARGUMENT`.
- An integer combined with a float computes in f64; floats follow IEEE 754 (dividing a float by zero gives an infinity or NaN, `%` is `fmod`).
  f32 values widen exactly; all float arithmetic is f64.
- Comparing an integer with a float is exact, whatever their magnitudes.
  NaN is unequal to everything (`<>` holds, every other comparison fails); -0 equals 0.
- Text and bytes support only comparisons, bytewise and unsigned. `bits(N)` supports `=` and `<>` with bits of the same N. `bool` supports `=`, `<>`, logic and use as a predicate.
- LEAST and GREATEST take numbers; COALESCE takes arguments of one type. When any argument is a float, the result is f64.
- The value IN tests and every item of its list must be mutually comparable, also when the value is NULL (`NULL IN (1, 'a')` is refused).
- Anything else is `INVALID_ARGUMENT` at planning.

A parameter takes the type of the column-typed operand it meets: the other side of a comparison, the value of IN or BETWEEN, the assigned column, or an enclosing operation's type.
Integers of different types combine as their common type for this purpose (the same type, else i64 if either is signed, else u64); arithmetic with a float makes f64, and with a float literal f64 even when every column is f32.
A parameter tested by BETWEEN against two typed bounds takes their common type by the same rule, whatever their order.
A parameter with no column-typed operand (`$1 < $2`, `$1 IN (1, 2)`, `$1 + 1 < 2`) is refused.
An integer parameter carries the 64-bit value of its frame; it is not narrowed to its column's width before it is compared or combined.
Literals keep the value they are written with: a float literal compared or combined with an f32 column is not rounded to f32, the f32 value widens to f64 and the comparison is exact (with an f32 column holding 16777216, `f32c < 16777216.5` holds and `f32c = 16777217.0` does not).
Rounding to f32, and the `OUT_OF_RANGE` check for a finite float beyond f32, happen only when the final value is assigned to an f32 column.

## NULL and absent blocks

Arithmetic with NULL is NULL, without an error even for division by zero.
Comparisons with NULL are unknown; AND, OR and NOT follow three-valued logic; `IS [NOT] NULL` is never unknown; IN and BETWEEN follow SQL.
A condition that is unknown is false.
COALESCE returns its first argument that is not NULL.
LEAST and GREATEST ignore NULL arguments and return NULL only if all are NULL (as PostgreSQL); a NaN argument makes the result NaN, and of equal values the first is kept.
For an absent block, EXISTS is false and every block column is NULL; chunk columns keep their values.

## Evaluation order

Operands evaluate left to right. AND and OR stop at the first operand that decides them, and COALESCE at its first value that is not NULL; their later operands raise no errors.
Every other operand is evaluated, so `NULL + 1 / 0` is an error.

## Assignment

All assignments of a statement read one immutable row, the block before the statement.
The value must fit the column: an integer outside the column's range, a finite float beyond f32, or text and bytes over the maximum are `OUT_OF_RANGE`.
Integers convert to floats by rounding to nearest; floats never convert to integers (a type error).
NULL into a column without NULL, text that is not UTF-8, and a column assigned twice are `INVALID_ARGUMENT`.

## Limits and errors

An expression has at most 256 nodes (values, columns, operators and list items) and 32 levels, and at most 32 nested parentheses; the caller may pass other limits.
Beyond them, and for anything the grammar refuses, parsing throws `ParseError` (`SYNTAX`).
Parsing recurses only through parentheses, so it is bounded by the depth limit.
Planning and evaluation throw `std::invalid_argument` (`INVALID_ARGUMENT`) and `std::out_of_range` (`OUT_OF_RANGE`).

## Statements

Every statement plans its expressions against the table's columns; each `$n` takes the type the plan gives it, and the server bounds its frame by that type before reading it.
`NOW()` is the commit time in writes and the start time in reads (docs/DESIGN-2.0.md E1).

## IF and computed values in writes

SET BLOCK keeps its parse when every value is a single value or parameter followed by a comma, IF or the end; otherwise it reads the whole value list again as assignments, so a statement without expressions keeps its parse, plan and write path (`cql::SetBlock::expressions` stays null).
DELETE BLOCK and SET BLOCK take `IF <condition>` after the values; IF VERSION combines with computed values but not with a condition.
`IF VERSION $n` takes the version from a u64 frame and runs on this path, so the plain path keeps its literal version.
The statement plans its expressions against the table's columns when it runs.
`ChunkStore::WriteBlockDecided` holds the chunk's statement lock from the read to the write: after the chunk's backlog it decodes the block, and the chunk columns when the plan names one, issues the commit time that `NOW()` reads and the write takes, evaluates, and writes through the same locked path as SET BLOCK and DELETE BLOCK.
It decodes only the block columns the plan reads, and takes the decision as a non-owning callable reference, which does not allocate.
A false or unknown condition answers `CONDITION_FAILED current=<chunk version>` and writes no WAL frame, revision or feed change; IF VERSION with computed values still answers VERSION_MISMATCH.
In a transaction the expressions read the transaction's copy of the chunk, or the block at the snapshot, before the chunk joins the write set, so a refused statement takes no write room and copies nothing.
A condition, or a value that reads a block or chunk column, adds the chunk to the read set.
`NOW()` is refused in a transaction's write, since its commit time is known only at COMMIT; a client passes the time as a parameter (decision).
A condition needs READ and WRITE, which WRITE alone gives.

## WHERE in reads

AGGREGATE and SCAN CHUNKS (E5, E6) evaluate WHERE once per present block; absent positions are not evaluated, since a read has no block to return or count there.
A chunk without chunk-column values reads the columns' defaults.
AGGREGATE adds every requested cell to a transaction's read set, as without WHERE: any block's write can change which blocks match.
SCAN CHUNKS without SINCE omits chunks with no match; with SINCE it lists every changed identity, with null data when no block matches, so a filtered copy converges; SINCE with WHERE needs WITH DATA, since the rows would otherwise ignore WHERE.
LIMIT counts visited identities, so the page's reads stay bounded; the reply adds `after`, the last visited identity, because the last row need not be.
