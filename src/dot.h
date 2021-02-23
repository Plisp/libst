#pragma once
#include <stdio.h>

void graph_begin(FILE *file)
{
	fprintf(file,
		"digraph g {\n"
		"  bgcolor=transparent;\n"
		"  node [shape=none];\n"
	);
}

void graph_link(FILE *file, const void *a, const char *port_a,
				const void *b, const char *port_b)
{
	fprintf(file, "  x%ld:%s -> x%ld:%s\n", (long)a, port_a, (long)b, port_b);
}

void graph_link_str(FILE *file, const void *a, const char *s, int len)
{
	fprintf(file, "  x%ld -> %.*s\n", (long)a, len, s);
}

void graph_table_begin(FILE *file, const void *o, const char *color)
{
	fprintf(file, "\n  x%ld [", (long)o);
	if(color)
		fprintf(file, "color=%s ", color);
	fprintf(file,
		"label=<<table border=\"0\" cellborder=\"1\" "
		"cellspacing=\"0\" cellpadding=\"6\" align=\"center\" port=\"body\">\n"
		"  <tr>\n"
	);
}

void graph_table_entry(FILE *file, const char *s, const char *port)
{
	fprintf(file, "    <td height=\"36\" width=\"25\" ");
	if(port)
		fprintf(file, "port= \"%s\"", port);
	fprintf(file, ">%s</td>\n", s);
}

void graph_table_end(FILE *file) {
	fprintf(file,
		"  </tr>\n</table>>];\n"
	);
}

void graph_end(FILE *file)
{
	fprintf(file, "}\n");
}
