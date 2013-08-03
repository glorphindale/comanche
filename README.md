# comanche

Comanche tribe needs a new leader.
Условия задания можно прочитать [на сайте Echo|http://www.echorussia.ru/jobs/serverside-june-2013.html]

## Usage

* Условие завершения потоков, корректное закрытие портов.
* Полностью отделить алгоритм от сетевой части - автомат, общающийся с сетью через очереди
  сообщений. Проблема - таймауты, решить через core.async?
* над send-msg нужна нахлобучка, которая абстрагирует идентификаторы от транспорта
* поставить лимиты на сокеты в 1 сообщение
* handle ctrl-c
* добавить состояние :exit для вывода узлов
* добавить эмуляцию медленных узлов
* защита рабочих потоков от исключений
## License

Copyright © 2013 glorphindale

Distributed under the Eclipse Public License, the same as Clojure.
