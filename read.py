from meta_ai_api import MetaAI
import os

path = os.getcwd()

dir_list = os.listdir(os.getcwd()+"/output")

print(dir_list)

for i in dir_list:
    with open(os.getcwd()+"/output/"+i, "r") as f:
        data = f.read()
        print(data)

ai = MetaAI()
prompt_template = {
    "prompt_template": """
You are an AI assistant specialized in analyzing Reddit user data. Your task is to analyze the given user's comment history and provide insights into their personality, interests, and behavior.

Given the user's comment history, please provide an analysis focusing on the following aspects:
Summarize a Reddit thread where people share humorous and relatable comments about a weird or taboo topic.
1. Personality Traits: Identify key personality traits based on the user's comments.
2. Interests & Passions: Determine the user's main interests and passions from their subreddit choices and comment content.
3. Communication Style: Describe how the user typically engages with others on Reddit.
4. Social Behavior: Infer the user's social interaction tendencies on the platform.
5. Recurring Themes: Identify any patterns or repeated themes in the user's comments.

For each aspect, provide a concise analysis supported by specific examples from the user's comment history when possible. Limit your total response to approximately 500 words.

User's comment history:
"""
}
message = prompt_template["prompt_template"] + data
response = ai.prompt(message=message)
print(response['message'])
