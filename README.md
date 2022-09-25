EasyStore - простая файловая хобби база данных на питоне

Главным файлом базы данных является файл с расширением .estore, он содержит информацию 
о SubStor-ах, которые хранят внутри себя записи.

Примеры использования:

    from easystore import EasyStore
    store = EasyStore("store.estore")
    store.create_sub_store("students", ["id[pk]", "name"])
    students_store = store.get_sub_store("students")
    students_store.insert_one(name="dan")
    student = students_store.get_one(name="dan")
    print(student.name, student.id) # dan 1
    students_store.delete_one()
    student = student.get_one(name="dan")
    print(student) # None

    