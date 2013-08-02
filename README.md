# comanche

Comanche tribe need a new leader.

## Usage

* Условие завершения потоков, корректное закрытие портов.
* Полностью отделить алгоритм от сетевой части - автомат, общающийся с сетью через очереди
  сообщений. Проблема - таймауты, решить через core.async?
* над send-msg нужна нахлобучка, которая абстрагирует идентификаторы от транспорта
* поставить лимиты на сокеты в 1 сообщение
* сохранить все фьючеры в ref, чтобы потом управлять узлами через REPL
* shutdown-agents
* handle ctrl-c
* добавить состояние :exit для вывода узлов
## License

Copyright © 2013 FIXME

Distributed under the Eclipse Public License, the same as Clojure.
