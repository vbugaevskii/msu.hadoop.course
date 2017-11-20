1. Компиляция программы проходила в среде разработки IntelijIdea с помощью
указания maven цели с параметром package, что соответвует запуску команды
"mvn package" в консоли. В результате работы в директории target появляется
исполняемый файл matrix-multiplier-1.0-SNAPSHOT.jar.

2. Запуск задачи на сервере
hadoop jar matrix-multiplier-1.0-SNAPSHOT.jar mm -conf config.xml \
    test/matrix_A test/matrix_B test/matrix_C

hadoop jar matrix-multiplier-1.0-SNAPSHOT.jar mm \
    -D mm.groups=10 -D mm.tags="ABC" -D mm.num-reducers=1 \
    test/matrix_A test/matrix_B test/matrix_C

В test/matrix_* содержится директория data (с значениями матрицы) и файл size
с размерами матрицы.