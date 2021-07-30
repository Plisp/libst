# An experimental text buffer structure

-   efficient range scans (sufficient for line iteration)
-   efficient insert(i, string), delete(i, n)
-   large file support (use virtual memory)

## undo support

-   oplog
    -   space and time inefficiency, esp. with piece tables all deleted
        data is reinserted
    -   all deleted text must be copied
-   descriptor level oplog (linked list)
    -   the same sequence design is wasteful for a tree structure
-   persistence, with transient model
    - oplog for small inserts

## SWMR parallelism

~~seqlock for reader invalidation, with sufficient safety guarantee
(linked-list)~~

-   snapshot by treating original text as immutable
-   functional updates (solves persistence too)

# NO

-   line indexing (...nothing yet)
-   byte indexing
-   multiple writers
-   while a generic replace operation seems conceptually elegant, it is
    likely to be less efficient than a deletion followed by insertion,
    as the insertion will use the same path as the deletion, and will
    occur at a newly-created descriptor boundary

## implicit mark maintenance

### index structure in add buffers - offset-\>piece start offsets

-   multiple kinds of mark logic re. insertion at mark and deletions
-   theoretical only, not good to mix \'hot\' code with mark maintenance
    -   simple to use an interval tree

## Atom patch

### storing strings in nodes

-   introduces an explicit distinction between old text and added text
    (old text may be mmap()-ed and cannot be edited directly)
-   this has merit in a mutable implementation, allowing edits within
    added text without creating new descriptors
    -   assuming such texts are small, which is reasonable but doesn\'t
        account for inserting command output, other files, etc.
-   undo is unwieldy
    -   either a structure such as an rrb-tree is required to keep
        string inserts persistent
    -   or any deleted text must be copied, which is wasteful

### the \'patch\' - tracking deleted ranges within nodes

-   unnatural explicit distinction between old text and added text
-   less than half the number of descriptors in a piece table, given a
    disjoint series of search/replace operations. n vs 2n+1
    -   there may actually be **more** descriptors. Consider deleting at
        the start and then end of the document. piece table: 1, patch: 2
-   benefit of less descriptors is reduced by lower possible fanout due
    to larger nodes (tracking old text sizes)

# tricks worth considering

-   transient model with refcounting
-   sentinel
    -   inline binary search within nodes (CSB-tree)
        -   updates take slightly more time due to touching all keys
            within modified nodes, but lookup dominates
    -   piece chains (Project Oberon)
-   allocate whole level at once, partial pointer elimination (CSB-tree)
    -   \[thought\] good for mutable structures, not so great with
        persistent structure sharing - good enough
-   radix tries, variable node sizes (ART, HOT)

# things not to try

-   node pooling (try performing less allocation)
-   impractical preallocation (scapegoat disaster)

## qttextedit (piece table)

line 165 is of interest, also search textfragmentdata
<https://code.woboq.org/qt5/include/qt/QtGui/5.12.2/QtGui/private/qtextdocument_p.h.html#QTextDocumentPrivate>
pooled red-black tree - first fragment in \*fragments contains a punned
Header
<https://code.woboq.org/qt5/include/qt/QtGui/5.12.2/QtGui/private/qfragmentmap_p.h.html#QFragmentMapData>

## gtktextbuffer (blocks)

<https://code.woboq.org/gtk/gtk/gtk/gtktextbuffer.h.html#_GtkTextBuffer>
b+tree of segmented gtktextlines
<https://code.woboq.org/gtk/gtk/gtk/gtktextbtree.c.html#_GtkTextBTree>
<https://code.woboq.org/gtk/gtk/gtk/gtktextbtree.h.html#_GtkTextLine>

## vscode (piece table)

red black tree
<https://github.com/microsoft/vscode/blob/main/src/vs/editor/common/model/pieceTreeTextBuffer/rbTreeBase.ts>

## atom (hybrid blocks)

patch structure
https://github.com/atom/superstring/blob/master/src/core/patch.h

## vim (blocks)

b+tree of lines
https://github.com/vim/vim/blob/master/src/memline.c

## emacs (gap buffer)

https://github.com/emacs-mirror/emacs/blob/master/src/insdel.c

## intellij (rope bonus round)

<https://github.com/JetBrains/intellij-community/blob/master/platform/util/strings/src/com/intellij/util/text/ImmutableText.java>
