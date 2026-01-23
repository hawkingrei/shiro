package generator

import (
	"fmt"
	"strings"

	"shiro/internal/schema"
	"shiro/internal/util"
)

const (
	// ColumnNullableProb is the chance to mark a column nullable.
	ColumnNullableProb = 20
	// ColumnIndexProb is the chance to add an index on a column.
	ColumnIndexProb = 30
	// PartitionCountExtraMax controls how many partitions above the minimum.
	PartitionCountExtraMax = 3
	// PartitionCountMin is the minimum number of partitions.
	PartitionCountMin = 2
)

// GenerateTable creates a randomized table definition.
func (g *Generator) GenerateTable() schema.Table {
	colCount := g.Rand.Intn(g.Config.MaxColumns-2) + 2
	cols := make([]schema.Column, 0, colCount+1)
	cols = append(cols, schema.Column{Name: "id", Type: schema.TypeBigInt, Nullable: false})

	for i := 0; i < colCount; i++ {
		col := schema.Column{
			Name:     fmt.Sprintf("c%d", i),
			Type:     g.randomColumnType(),
			Nullable: util.Chance(g.Rand, ColumnNullableProb),
			HasIndex: util.Chance(g.Rand, ColumnIndexProb),
		}
		cols = append(cols, col)
	}

	partitioned := false
	partitionCount := 0
	if g.Config.Features.PartitionTables && util.Chance(g.Rand, g.Config.Weights.Features.PartitionProb) {
		partitioned = true
		partitionCount = g.Rand.Intn(PartitionCountExtraMax) + PartitionCountMin
	}

	return schema.Table{
		Name:           g.NextTableName(),
		Columns:        cols,
		HasPK:          true,
		NextID:         1,
		Partitioned:    partitioned,
		PartitionCount: partitionCount,
	}
}

// CreateTableSQL renders a CREATE TABLE statement for a schema table.
func (g *Generator) CreateTableSQL(tbl schema.Table) string {
	parts := make([]string, 0, len(tbl.Columns)+2)
	for _, col := range tbl.Columns {
		line := fmt.Sprintf("%s %s", col.Name, col.SQLType())
		if !col.Nullable {
			line += " NOT NULL"
		}
		parts = append(parts, line)
	}
	if tbl.HasPK {
		parts = append(parts, "PRIMARY KEY (id)")
	}
	for _, col := range tbl.Columns {
		if col.HasIndex {
			parts = append(parts, fmt.Sprintf("INDEX idx_%s (%s)", col.Name, col.Name))
		}
	}
	stmt := fmt.Sprintf("CREATE TABLE %s (%s)", tbl.Name, strings.Join(parts, ", "))
	if tbl.Partitioned && tbl.PartitionCount > 1 {
		stmt += fmt.Sprintf(" PARTITION BY HASH(id) PARTITIONS %d", tbl.PartitionCount)
	}
	return stmt
}

// CreateIndexSQL emits a CREATE INDEX statement and updates table metadata.
func (g *Generator) CreateIndexSQL(tbl *schema.Table) (string, bool) {
	candidates := make([]*schema.Column, 0, len(tbl.Columns))
	for i := range tbl.Columns {
		col := &tbl.Columns[i]
		if col.HasIndex {
			continue
		}
		candidates = append(candidates, col)
	}
	if len(candidates) == 0 {
		return "", false
	}
	col := candidates[g.Rand.Intn(len(candidates))]
	col.HasIndex = true
	indexName := fmt.Sprintf("idx_%s_%d", col.Name, g.indexSeq)
	g.indexSeq++
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s)", indexName, tbl.Name, col.Name), true
}

// CreateViewSQL emits a CREATE VIEW statement from a generated query.
func (g *Generator) CreateViewSQL() string {
	query := g.GenerateSelectQuery()
	if query == nil {
		return ""
	}
	if len(query.With) > 0 {
		cteEnabled := g.Config.Features.CTE
		g.Config.Features.CTE = false
		query = g.GenerateSelectQuery()
		g.Config.Features.CTE = cteEnabled
		if query == nil {
			return ""
		}
	}
	query = query.Clone()
	query.Items = ensureUniqueAliases(query.Items)
	viewName := g.NextViewName()
	return fmt.Sprintf("CREATE VIEW %s AS %s", viewName, query.SQLString())
}

func ensureUniqueAliases(items []SelectItem) []SelectItem {
	used := map[string]int{}
	out := make([]SelectItem, len(items))
	for i, item := range items {
		base := strings.TrimSpace(item.Alias)
		if base == "" {
			if col, ok := item.Expr.(ColumnExpr); ok {
				base = col.Ref.Name
			} else {
				base = fmt.Sprintf("c%d", i)
			}
		}
		if count, ok := used[base]; ok {
			count++
			used[base] = count
			item.Alias = fmt.Sprintf("%s_%d", base, count)
		} else {
			used[base] = 0
			item.Alias = base
		}
		out[i] = item
	}
	return out
}

// AddCheckConstraintSQL emits a CHECK constraint for a table.
func (g *Generator) AddCheckConstraintSQL(tbl schema.Table) string {
	predicate := g.GeneratePredicate([]schema.Table{tbl}, g.maxDepth-1, false, 0)
	name := g.NextConstraintName("chk")
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)", tbl.Name, name, g.exprSQL(predicate))
}

// AddForeignKeySQL emits a FOREIGN KEY constraint when possible.
func (g *Generator) AddForeignKeySQL(state *schema.State) string {
	if state == nil || len(state.Tables) < 2 {
		return ""
	}
	child := state.Tables[g.Rand.Intn(len(state.Tables))]
	parent := state.Tables[g.Rand.Intn(len(state.Tables))]
	if child.Name == parent.Name {
		return ""
	}
	if child.Partitioned || parent.Partitioned {
		return ""
	}
	childCol, parentCol := g.pickForeignKeyColumns(child, parent)
	if childCol.Name == "" || parentCol.Name == "" {
		return ""
	}
	name := g.NextConstraintName("fk")
	return fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
		child.Name, name, childCol.Name, parent.Name, parentCol.Name,
	)
}
