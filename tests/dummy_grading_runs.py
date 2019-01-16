import src.constants.keys as key

one_student_job = {key.STUDENTS_ENV: [{"netid": "test net id"}]}
one_student_and_pre = {key.PRE_PROCESSING_ENV: {"type": "pre"}, key.STUDENTS_ENV: [{"netid": "test net id"}]}
one_student_and_post = {key.STUDENTS_ENV: [{"netid": "test net id"}], key.POST_PROCESSING_ENV: {"type": "post"}}
one_student_and_both = {key.PRE_PROCESSING_ENV: {"type": "pre"}, key.STUDENTS_ENV: [{"netid": "test net id"}],
                        key.POST_PROCESSING_ENV: {"type": "post"}}
