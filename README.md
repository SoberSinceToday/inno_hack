INNO HACK

Наш алгоритм приводит все данные к единому формату, затем ищет совпадения по адресам между первой и второй таблицей и по email'ам для первой и третьей, объединяет дублирующиееся записи в требуемом формате и сохраняет их
У нашего решения огромный потенциал развития, например:
  Сравнение данных по алгоритму Левенштейна/исправление предобученной LLM моделью орфографических ошибок, применение эмбеддингов и сравнение по алгоритму косинусных схожестей
  Ускорение решения
    Замена синтаксических конструкций(например for i1, raw1 in repeated_values_df1.iterrows() заменить на for address, uid in zip(repeated_values_df1['address'].tolist(), repeated_values_df1['uid'].tolist() и т.п.)
    Внедрение параллелизации 
    Переход с pandas на polars/работу неосредственно с БД/spark
  Применение векторизации и алгоритмов кластеризации для нахождения совпадений
  Поиск более оптимального метода работы с данными для нахождения большего количества решений в кратчайшее время
