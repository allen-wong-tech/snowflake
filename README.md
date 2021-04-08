# Snowflake Dynamic Data Masking Demo

[24 minute Youtube demo of these scripts](https://youtu.be/94d3jID252A)

We show how to meet this use case on Snowflake: The **Human Resources team** has the following requirements around their **Personally Identifiable Information (PII)** of employee salary data:

1. A regular employee is allowed to see his/her own data
2. A manager can see data related to his/her direct and in-direct reports
3. Members of HR can see everybody’s data except other HR people
4. Members Finance can see data rolled up by department
