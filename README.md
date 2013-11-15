# comanche (en)

Bully algorithm - algorithm for cluster leader election, see ["Bully algorithm"](http://en.wikipedia.org/wiki/Bully_algorithm).
Implemented in Clojure using zeromq as a transport layer.

Bully algorithm does not handle split-brain problems in any way, use in production at your own risk.

# comanche (ru)

Племени команчей нужен новый вождь.
Реализация алгоритма задиры [Bully algorithm](http://en.wikipedia.org/wiki/Bully_algorithm) на 
Clojure с использованием zeromq.
Алгортим задиры не является надёжным средством выбора лидера в кластере, и не защищает от ошибок
split-brain. За последствия использования в продакшне команчи ответственности не несут!

## Описание

Каждый узел представляет из себя два потока (future). Один поток принимает сообщения, второй
занимается опросом других узлов. Потоки разделют atom с общим состоянием: номер узла-вождя и текущий
статус узла.

Для посылки сообщений используется ZeroMQ, шаблон Request-Reply. Узлы обмениваются сообщениями в
формате "id-узла-автора:сообщение".

Пространства имён:
* comanche.smoke - сетевая часть
* comanche.signals - функции для посылки сообщений кластеру
* comanche.constants 
* comanche.chief - алгоритм, работа с несколькими узлами и :main

Для упрощения тестирования в каждом процессе может быть запущено несколько узлов кластера.

## Управление

Конфигурационный файл cluster.conf содержит вектор map-ов:
[{:id 0 :location "tcp://127.0.0.1:9000"} ... ]
Для создания нового кластера можно использовать (spit "cluster.conf" (gen-new-cluster 8))

Для упрощения тестирования программу можно запускать с помощью нескольких опций командной строки:
* lein run -f - запускает в одном процессе сразу все узлы кластера
* lein run 0 2 4 - запускает в одном процессе указанные узлы кластера 
* lein run 1-5 - запускает в одном процессе диапазон узлов (включительно)

Если запустить lein repl, можно отдавать кластеру команды через пространства имён smoke и signals.
* (signals/send-ping 0 (cluster 7)) для посылки сообщений PING узлу №7

Помимо команд протокола, добавлена поддержка двух полезных функций. Например:
* (smoke/send-msg 0 (cluster 7) "0:EXIT!") для полной остановки узла №7
* (smoke/send-msg 0 (cluster 7) "0:REPORT!") для получения текущего состояния узла №7
* (signals/cluster-status cluster) для получения мнения кластера о своём состоянии (запрос REPORT! ко
  всем узлам и применение frequencies к результату)

## Нюансы алгоритма

* Если связь между всеми узлами нарушена - все превратятся в вождей и не починятся, пока какой-то из
  узлов не начнёт выборы (split-brain problem)

## TODO
* Полностью отделить алгоритм от сетевой части - автомат, общающийся с сетью через очереди
  сообщений. Проблема - таймауты, решить через core.async?
* Добавить эмуляцию медленных узлов
* Лучшая диагностика ошибок - неудачные bind, connect, etc
* Отловить баг с IllegalStateException: bottom

## License

Copyright © 2013 glorphindale

Distributed under the Eclipse Public License, the same as Clojure.
