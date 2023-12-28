from sqlalchemy import and_, func, desc, select

from conf.db_conn import session
from conf.models import Teacher, Student, Group, Grade, Subject


def select_1():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    """
    best_five = (
        session.query(
            Student.fullname, func.round(func.avg(Grade.grade), 2).label("avg_grade")
        )
        .select_from(Grade)
        .join(Student)
        .group_by(Student.id)
        .order_by(desc("avg_grade"))
        .limit(5)
        .all()
    )
    return best_five


def select_2(subject_id):
    """
    Знайти студента із найвищим середнім балом з певного предмета.
    """
    best = (
        session.query(
            Student.fullname, func.round(func.avg(Grade.grade), 2).label("avg_grade")
        )
        .join(Grade)
        .filter(Grade.subject_id == subject_id)
        .group_by(Student.id)
        .order_by(desc("avg_grade"))
        .first()
    )
    return best


def select_3(subject_id):
    """
    Знайти середній бал у групах з певного предмета.
    """
    result = (
        session.query(
            Group.name, func.round(func.avg(Grade.grade), 2).label("avg_grade")
        )
        .select_from(Group)
        .join(Student, Group.students)
        .join(Grade, Student.grade)
        .filter(Grade.subject_id == subject_id)
        .group_by(Group.name)
        .order_by(desc("avg_grade"))
        .all()
    )
    return result


def select_4():
    """
    Знайти середній бал на потоці (по всій таблиці оцінок).
    """
    result = session.query(
        func.round(func.avg(Grade.grade), 2).label("avg_grade")
    ).all()
    return result


def select_5(teacher_id):
    """
    Знайти, які курси читає певний викладач.
    """
    result = (
        session.query(Teacher.fullname, Subject.name)
        .select_from(Subject)
        .join(Teacher, Subject.teacher)
        .filter(Subject.teacher_id == teacher_id)
        .group_by(Teacher.fullname, Subject.name)
        .all()
    )
    return result


def select_6(group_id):
    """
    Знайти список студентів у певній групі.
    """
    result = (
        session.query(Group.name, Student.fullname)
        .join(Group, Student.group)
        .filter(Student.group_id == group_id)
        .group_by(Group.name, Student.fullname)
        .limit(10)
        .all()
    )
    return result


def select_7(group_id, subject_id):
    """
    Знайти оцінки студентів у окремій групі з певного предмета.
    """
    result = (
        session.query(Student.fullname, Group.name, Subject.name, Grade.grade)
        .join(Group, Student.group)
        .join(Grade, (Student.id == Grade.student_id))
        .join(Subject, (Grade.subject_id == Subject.id))
        .filter(Student.group_id == group_id)
        .filter(Grade.subject_id == subject_id)
        .limit(5)
        .all()
    )
    return result


def select_8(teacher_id):
    """
    Знайти середній бал, який ставить певний викладач зі своїх предметів.
    """
    result = (
        session.query(
            Teacher.fullname,
            Subject.name,
            func.round(func.avg(Grade.grade), 2).label("avg_grade"),
        )
        .select_from(Teacher)
        .join(Subject, Teacher.id == Subject.teacher_id)
        .join(Grade, Subject.id == Grade.subject_id)
        .filter(Subject.teacher_id == teacher_id)
        .group_by(Teacher.fullname, Subject.name)
        .all()
    )
    return result


def select_9(student_id):
    """
    Знайти список курсів, які відвідує певний студент.
    """
    result = (
        session.query(Student.fullname, Subject.name)
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Subject.id == Grade.subject_id)
        .filter(Student.id == student_id)
        .distinct()
        .all()
    )
    return result


def select_10(student_id, teacher_id):
    """
    Список дисциплін, які відвідує певний студент, та фамілія викладача.
    """
    result = (
        session.query(Student.fullname, Subject.name, Teacher.fullname)
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Subject.id == Grade.subject_id)
        .join(Teacher, Subject.teacher_id == Teacher.id)
        .filter(Student.id == student_id)
        .filter(Teacher.id == teacher_id)
        .distinct()
        .all()
    )
    return result


def select_11(student_id, teacher_id):
    """
    Середній бал, який певний викладач ставить певному студентові.
    """
    result = (
        session.query(
            func.round(func.avg(Grade.grade), 2).label("avg_grade"),
            Teacher.fullname.label("teacher_name"),
            Student.fullname.label("student_name"),
        )
        .join(Student, Grade.student_id == Student.id)
        .join(Subject, Grade.subject_id == Subject.id)
        .join(Teacher, Subject.teacher_id == Teacher.id)
        .filter(Student.id == student_id)
        .filter(Teacher.id == teacher_id)
        .group_by(Teacher.fullname, Student.fullname)
        .first()
    )

    if result:
        return result.avg_grade, result.teacher_name, result.student_name
    else:
        return None


def select_12(subject_id, group_id):
    """
    Оцінки студентів у певній групі з певного предмета на останньому занятті.
    """
    subquery = (
        select(Grade.grade_date)
        .join(Student)
        .join(Group)
        .where(and_(Grade.subject_id == subject_id, Group.id == group_id))
        .order_by(desc(Grade.grade_date))
        .limit(1)
        .scalar_subquery()
    )

    result = (
        session.query(
            Subject.name, Student.fullname, Group.name, Grade.grade_date, Grade.grade
        )
        .select_from(Grade)
        .join(Student)
        .join(Subject)
        .join(Group)
        .filter(
            and_(
                Subject.id == subject_id,
                Group.id == group_id,
                Grade.grade_date == subquery,
            )
        )
        .order_by(desc(Grade.grade_date))
        .all()
    )

    return result


if __name__ == "__main__":
    separator = "-" * 50
    print(select_1.__doc__.strip())
    print(select_1())
    print(separator)

    print(select_2.__doc__.strip())
    print(select_2(1))
    print(separator)

    print(select_3.__doc__.strip())
    print(select_3(2))
    print(separator)

    print(select_4.__doc__.strip())
    print(select_4())
    print(separator)

    print(select_5.__doc__.strip())
    print(select_5(2))
    print(separator)

    print(select_6.__doc__.strip())
    print(select_6(2))
    print(separator)

    print(select_7.__doc__.strip())
    print(select_7(1, 1))
    print(separator)

    print(select_8.__doc__.strip())
    print(select_8(1))
    print(separator)

    print(select_9.__doc__.strip())
    print(select_9(1))
    print(separator)

    print(select_10.__doc__.strip())
    print(select_10(3, 1))
    print(separator)

    print(select_11.__doc__.strip())
    print(select_11(1, 1))
    print(separator)

    print(select_12.__doc__.strip())
    print(select_12(1, 1))
    print(separator)
