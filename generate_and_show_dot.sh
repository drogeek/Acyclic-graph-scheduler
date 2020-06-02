#!/bin/bash

/usr/bin/dot -Tpng executable_graph.dot > graph.png
/usr/bin/xdg-open graph.png
