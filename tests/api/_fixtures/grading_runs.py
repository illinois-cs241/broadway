one_student_job = {"students_env": [{"netid": "test net id"}]}
one_student_and_pre = {
    "pre_processing_env": {"type": "pre"},
    "students_env": [{"netid": "test net id"}],
}
one_student_and_post = {
    "students_env": [{"netid": "test net id"}],
    "post_processing_env": {"type": "post"},
}
one_student_and_both = {
    "pre_processing_env": {"type": "pre"},
    "students_env": [{"netid": "test net id"}],
    "post_processing_env": {"type": "post"},
}

two_student_job = {
    "students_env": [{"netid": "student id 1"}, {"netid": "student id 2"}]
}


def generate_n_student_jobs(n):
    return {"students_env": [{"netid": "test net id"}] * n}
