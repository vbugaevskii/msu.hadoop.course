1. Компиляция программы проходила в среде разработки IntelijIdea с помощью
указания maven цели с параметром package, что соответвует запуску команды
"mvn package" в консоли. В результате работы в директории target появляется
исполняемый файл wordcount-1.0-SNAPSHOT.jar.

2. Запуск задачи на сервере
hadoop jar wordcount-1.0-SNAPSHOT.jar wordcount \
    /example/data/gutenberg/davinci.txt \
	/example/data/wordcountout
