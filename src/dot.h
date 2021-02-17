#include <stdio.h>

void graph_begin(FILE *file)
{
	fprintf(file,
		"digraph g {\n"
		"  bgcolor=transparent;\n"
		"  node [shape=none];\n"
	);
}

void graph_link(FILE *file, void *a, void *b)
{
	fprintf(file, "  x%ld -> x%ld\n", (long)a, (long)b);
}

void graph_link_str(FILE *file, void *a, const char *s, int len)
{
	fprintf(file, "  x%ld -> %.*s\n", (long)a, len, s);
}

void graph_table_begin(FILE *file, void *o)
{
	fprintf(file,
		"\n  x%ld [label=<<table border=\"0\" cellborder=\"1\" "
		"cellspacing=\"0\" cellpadding=\"6\" align=\"center\" port=\"body\">\n"
		"  <tr>\n", (long)o
	);
}

void graph_table_entry(FILE *file, const char *s, const char *port)
{
	if(port)
		fprintf(file,
			"    <td height=\"36\" width=\"25\" port=\"%s\">%s</td>\n",
			                                           port, s
		);
	else
		fprintf(file,
			"    <td height=\"36\" width=\"25\">%s</td>\n",
			                                     s
		);
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
