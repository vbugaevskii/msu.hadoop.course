1. Компиляция программы проходила в среде разработки IntelijIdea с помощью
указания maven цели с параметром package, что соответвует запуску команды
"mvn package" в консоли. В результате работы в директории target появляется
исполняемый файл sparse-matrix-generator-1.0-SNAPSHOT.jar.

2. Запуск задачи на сервере
hadoop jar sparse-matrix-generator-1.0-SNAPSHOT.jar \
    mgen -conf config.xml output

hadoop jar sparse-matrix-generator-1.0-SNAPSHOT.jar mgen \
    -D mgen.row-count=20000 -D mgen.column-count=20000 -D mgen.seed=8888 \
    -D mgen.num-mappers=30 -D mgen.sparsity=0.0 output

На выходе в директории output оказывается директория matrix.data (данные) и
файл matrix.size (размеры матрицы)