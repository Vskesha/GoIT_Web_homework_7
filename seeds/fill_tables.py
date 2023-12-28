from random import randint

from faker import Faker
from sqlalchemy.exc import SQLAlchemyError

from conf.db_conn import session
from conf.models import Teacher, Student, Group, Subject, Grade

NUMBER_OF_TEACHERS = 5
NUMBER_OF_STUDENTS = 50
AVG_NUMBER_OF_GRADES = 20
SUBJECTS = {
    "математика",
    "література",
    "англійська",
    "історія",
    "географія",
    "біологія",
    "хімія",
    "фізика",
}
GROUPS = ["БЛБ-31", "БЛБ-32", "БЛБ-33"]
fake = Faker("uk-UA")


def seed_students():
    for _ in range(NUMBER_OF_STUDENTS):
        student = Student(fullname=fake.name(), group_id=randint(1, len(GROUPS)))
        session.add(student)


def seed_teachers():
    for _ in range(NUMBER_OF_TEACHERS):
        teacher = Teacher(fullname=fake.name())
        session.add(teacher)


def seed_subjects():
    for subject_name in SUBJECTS:
        subject = Subject(name=subject_name, teacher_id=randint(1, NUMBER_OF_TEACHERS))
        session.add(subject)


def seed_groups():
    for group_name in GROUPS:
        group = Group(name=group_name)
        session.add(group)


def seed_grades():
    for i in range(1, NUMBER_OF_STUDENTS + 1):
        for _ in range(randint(AVG_NUMBER_OF_GRADES - 3, AVG_NUMBER_OF_GRADES + 3)):
            grade = Grade(
                grade=randint(1, 12),
                grade_date=fake.date_between(start_date="-1y"),
                student_id=i,
                subject_id=randint(1, len(SUBJECTS)),
            )
            session.add(grade)


if __name__ == "__main__":
    try:
        seed_students()
        seed_teachers()
        seed_subjects()
        seed_groups()
        seed_grades()
        session.commit()
    except SQLAlchemyError as e:
        print(e)
        session.rollback()
    finally:
        session.close()
