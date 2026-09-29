# Python Interview Questions – Detailed Notes

Sep 30, 2026 · @Sunil Patil

## Overview

These notes cover about 90 Python interview questions for data engineering roles, grouped into 12 topics. They follow a 53-minute video by Narendra Kumar, a senior data architect with 12 years in IT. He is a Microsoft Certified Architect and a Databricks MVP.

- **Audience:** freshers through data engineers and data architects.
- **Use:** revision alongside his 6-hour Python course and its free notebooks (linked in the video description).
- **Tip from the speaker:** the basics are rarely asked directly. Lists, dictionaries, sets, OOP, error handling and logging get the most interview time.

## 1. Python basics

**Q: How is Python code executed? What is the Python interpreter?**

The interpreter reads the code line by line and runs each line as it goes. It creates no intermediate file.

- **Compiled languages** (for example C or Java) turn the code into an executable file first, then run that file.
- **Python** is interpreted, so it runs the source directly.

**Q: What is `None`?**

`None` is a reserved keyword meaning "no value". You can assign it to a variable (`x = None`). Functions, objects and APIs also return `None` when they have nothing to give back.

**Q: How do you check a variable's type?**

Use `type()`. For example, `type(x)` returns `<class 'int'>`.

**Q: How do you convert one type to another?**

Call the target type's function:

```python
int("42")     # string -> int  => 42
str(42)       # int -> string  => "42"
float("3.5")  # string -> float
```

Conversion works only when the value is valid for the target type. For example, `int("abc")` raises an error. (The video calls this a type error; Python actually raises `ValueError` here.)

## 2. Operators, conditionals and indentation

**Q: How do you combine two conditions so an action runs only when both are true?**

Use the logical operators:

| Operator | True when | Example |
| --- | --- | --- |
| `and` | both conditions are true | `if a > 0 and b > 0:` |
| `or` | at least one condition is true | `if a > 0 or b > 0:` |
| `not` | the condition is false (it flips the result) | `if not done:` |

**Q: What conditional statements does Python have?**

- **`if`**: runs a block only when the condition is true.
- **`if … else`**: runs one block when the condition is true and another when it is false.
- **`if … elif … else` (ladder)**: checks conditions in order and runs the first one that is true. If none match, the `else` block runs.

```python
if marks >= 90:
    grade = "A"
elif marks >= 75:
    grade = "B"
else:
    grade = "C"
```

**Q: What is indentation and why does it matter?**

Indentation is the leading whitespace that places child statements one level to the right of their parent. Python uses it instead of the braces `{}` other languages use. The interpreter reads it to know which statements belong to which block, so wrong indentation causes errors or wrong behaviour.

## 3. Loops and control flow

**Q: What types of loops does Python have?**

- **`for` loop**: use it for a known number of iterations or to iterate over a list or other sequence.
- **`while` loop**: use it to repeat while a condition holds, when you don't know how many iterations it will take.

**Q: How does `range()` work? How do you skip numbers or control the step size?**

| Call | Output | Meaning |
| --- | --- | --- |
| `range(5)` | 0, 1, 2, 3, 4 | from 0 to one less than the argument |
| `range(1, 5)` | 1, 2, 3, 4 | from start to one less than stop |
| `range(0, 10, 2)` | 0, 2, 4, 6, 8 | the third argument is the step size |

**Q: What does `break` do? How do you stop a loop early?**

`break` exits the loop immediately. No further iterations run.

**Q: What does `continue` do? How do you skip certain iterations?**

`continue` skips the rest of the current iteration and moves on to the next one.

**Q: What is the difference between `break` and `continue`?**

- `break` ends the whole loop.
- `continue` skips only the rest of the current iteration, and the loop keeps going.

```python
for i in range(10):
    if i == 3:
        continue   # skip 3
    if i == 7:
        break      # stop at 7
    print(i)       # 0 1 2 4 5 6
```

**Q: What is `pass`?**

`pass` is a placeholder that does nothing. Use it where the syntax needs a statement but you'll write the code later, such as `def todo(): pass`.

## 4. Strings and text processing

| Question | Answer | Example |
| --- | --- | --- |
| Create a multi-line string | Wrap it in triple quotes | `s = """line 1\nline 2"""` |
| Length of a string | `len()` | `len("hello")` gives 5 |
| Character at a position | Index from 0 in square brackets | `s[0]` gives the first character |
| Last or second-last character | Negative index | `s[-1]` gives the last character, `s[-2]` the second-last |
| Substring by position | Slice `[start:end]` (end is excluded) | `s[0:5]` |
| Remove leading/trailing spaces | `strip()` | `"  hi  ".strip()` gives `"hi"` |
| Check if a word exists | `find()` returns the position, or -1 if not found | `s.find("data")` |
| Replace a word | `replace(old, new)` | `s.replace("cat", "dog")` |
| Join two strings | `+` operator | `"Hello " + name` |
| Insert variables into a string | f-string: `f` prefix plus `{var}` | `f"Hi {name}, age {age}"` |
| Split a sentence into words | `split()` (splits on spaces by default) | `"a b c".split()` gives `['a', 'b', 'c']` |

Tip: for a simple yes/no existence check, `"data" in s` returns `True` or `False` and is often cleaner than `find()`.

## 5. Lists, tuples, dictionaries and sets

The speaker calls this the most important section. Expect several questions from it.

**Q: What are a list, tuple, dictionary and set? How do they differ?**

| Type | Syntax | Ordered | Mutable | Duplicates | Use when |
| --- | --- | --- | --- | --- | --- |
| List | `[1, 2, 2]` | Yes | Yes | Allowed | You need an ordered, changeable collection |
| Tuple | `(1, 2, 2)` | Yes | No | Allowed | The group of values should never change |
| Dictionary | `{"a": 1}` | Yes (insertion order) | Yes | Keys must be unique | You store key–value pairs |
| Set | `{1, 2}` | No | Yes | Not allowed (ignored) | You need unique values or set maths |

**List vs set:** a list keeps insertion order and allows duplicates. A set keeps only unique values and has no guaranteed order. (The video says sets tend to look sorted; don't rely on that.)

### Lists

**Q: How do you create a list?** Put comma-separated items in square brackets: `nums = [1, 2, 3]`.

**Q: Which list functions have you used?**

| Function | What it does |
| --- | --- |
| `len(lst)` | Number of items |
| `lst[i]` | Item at index i |
| `lst.extend(other)` | Adds all items from another list |
| `lst.insert(i, x)` | Inserts x at position i |
| `lst.pop()` | Removes and returns the last item (`pop(i)` removes the item at index i) |
| `lst.remove(x)` | Removes the first occurrence of x |
| `lst.sort()` | Sorts in place |
| `lst.reverse()` | Reverses in place |

**Q: What is a list comprehension?** It is a one-line way to build a new list by applying an operation to each item of an existing list:

```python
squares = [x * x for x in nums]
evens   = [x for x in nums if x % 2 == 0]   # with a filter
```

**Q: How do you get each item together with its position? (`enumerate`)**

```python
for i, name in enumerate(["Asha", "Ravi"]):
    print(i, name)   # 0 Asha, 1 Ravi
```

**Q: How do you pair up items from two lists in order? (`zip`)**

`zip` combines the items at matching positions into tuples:

```python
names = ["Narendra", "Asha", "Ravi"]
ages  = [30, 25, 40]
for name, age in zip(names, ages):
    print(name, age)
```

### Dictionaries

**Q: How do you create a dictionary?** Put `key: value` pairs, separated by commas, in curly braces: `emp = {"name": "Ravi", "age": 40}`.

**Q: How do you read a value? Which way is safer?**

- `emp["name"]` raises a `KeyError` if the key is missing.
- `emp.get("name")` returns `None` if the key is missing, with no error. This is the better choice for error handling. `get` also accepts a default, as in `emp.get("city", "NA")`.

Mention both ways without being asked. Interviewers often follow up with "which is better?"

**Q: How do you get all keys or all values?** Use `emp.keys()` and `emp.values()`.

**Q: How do you loop over key–value pairs?**

```python
for key, value in emp.items():
    print(key, value)
```

**Q: How do you create a nested dictionary?** Use a dictionary as a value:

```python
emp = {"name": "Ravi", "address": {"city": "Pune", "pin": 411001}}
emp["address"]["city"]   # "Pune"
```

### Sets (scenario questions)

| Task | Solution | Example |
| --- | --- | --- |
| Remove duplicates from a list | Convert it to a set | `set(lst)` |
| Combine two lists without duplicates | Union `\|` | `set(a) \| set(b)` |
| Keep only the items common to both | Intersection `&` | `set(a) & set(b)` |
| Keep items in A that are not in B | Difference `-` | `set(a) - set(b)` |

These operators work only on sets, so convert lists with `set()` first.

## 6. Functions, scope and functional tools

**Q: How do you write reusable code?** Put it in a function and call the function wherever you need it.

**Q: How do you define a function, and give an argument a default value?**

```python
def greet(name, greeting="Hello"):   # greeting has a default value
    return f"{greeting}, {name}"

greet("Asha")            # "Hello, Asha"
greet("Asha", "Hi")      # "Hi, Asha"
```

**Q: How do you accept any number of arguments? (`*args` and `**kwargs`)**

- `*args` collects any number of positional values into a tuple.
- `**kwargs` collects any number of named (`key=value`) arguments into a dictionary. Read them with `.items()`.

```python
def total(*args):
    return sum(args)          # total(1, 2, 3, 4) gives 10

def show(**kwargs):
    for k, v in kwargs.items():
        print(k, v)           # show(name="Ravi", age=40)
```

### Variable scope

- **Can you use a variable created inside a function from outside it?** No. Its scope is local, so it exists only while the function runs.
- **Can you read a variable defined outside, inside a function?** Yes. These are global variables.
- **How do you change a global variable inside a function?** Declare it with the `global` keyword first:

```python
count = 0
def increment():
    global count
    count += 1
```

### Lambda, map, filter and reduce

**Q: What is a lambda expression?** It is a one-line anonymous function written as `lambda inputs: output`. Assigning it to a variable makes that variable callable.

```python
square = lambda x: x * x      # square(4) gives 16
```

| Function | What it does | Returns | Example with `nums = [1, 2, 3, 4]` |
| --- | --- | --- | --- |
| `map` | Applies a function to every item | A new sequence of the same length | `list(map(lambda x: x * 2, nums))` gives `[2, 4, 6, 8]` |
| `filter` | Keeps only items that meet a condition | A shorter sequence | `list(filter(lambda x: x % 2 == 0, nums))` gives `[2, 4]` |
| `reduce` | Combines all items one after another into one result | A single value | `reduce(lambda a, b: a + b, nums)` gives `10` |

`reduce` must be imported with `from functools import reduce`.

**Q: What is the difference between `map` and `reduce`?** `map` returns a transformed item for every input item. `reduce` combines the items step by step, as in 1 + 2 + 3 + 4, and returns one final value.

## 7. Classes, objects and inheritance

This section gets many questions, including tricky ones.

### Core concepts

| Term | Meaning | Car example |
| --- | --- | --- |
| Class | A blueprint that defines attributes and behaviour | `Car` |
| Object | A real instance of the class with actual values | `my_car = Car("Tata", "Nexon")` |
| Attribute | A variable stored on the object | `brand`, `model` |
| Method | A function defined inside the class | `honk()` |
| `__init__` | Initialiser that runs automatically when an object is created, usually to set attributes | `def __init__(self, brand, model):` |
| `self` | A reference to the current object, the instance being created or used | `self.brand = brand` |

```python
class Car:
    def __init__(self, brand, model):
        self.brand = brand
        self.model = model

    def honk(self):                  # methods take self as the first parameter
        print(f"{self.brand} says beep!")

my_car = Car("Tata", "Nexon")      # creates an object; __init__ runs
print(my_car.brand)                # read an attribute: object.attribute
my_car.model = "Harrier"           # change an attribute
my_car.honk()
```

### Inheritance

**Q: What is inheritance and how do you create a subclass?** A subclass inherits all the attributes and methods of its parent without redefining them. Put the parent's name in brackets after the class name:

```python
class Vehicle:
    def __init__(self, color):
        self.color = color
    def honk(self):
        print("Beep")

class Car(Vehicle):                 # Car inherits from Vehicle
    def honk(self):                  # method overriding
        super().honk()               # call the parent's version
        print("Car horn!")
```

- **Method overriding:** a subclass defines a method with the same name as one in the parent. The subclass version replaces the parent's.
- **`super()`:** calls the parent class's method from inside the subclass, for example `super().__init__(color)`.

**Q: What types of inheritance does Python support?**

| Type | Structure |
| --- | --- |
| Single | One parent, one child (A → B) |
| Multi-level | A chain: grandparent → parent → child (A → B → C) |
| Hierarchical | One parent, several children (A → B, A → C) |
| Multiple | One child with several parents (A + B → C) |
| Hybrid | A mix of the above, for example A → B and C, then B + C → D (rare) |

### Access modifiers

| Modifier | Syntax | Access |
| --- | --- | --- |
| Public | `name` | Anywhere, inside or outside the class |
| Protected | `_name` | Meant for the class and its subclasses. This is only a convention: Python still allows outside access |
| Private | `__name` | Can't be accessed directly outside the class, including from subclasses |

**Q: How do you stop an attribute being accessed from outside the class?** Prefix it with a double underscore (`self.__salary`). Python renames it internally (name mangling to `_ClassName__salary`), so direct access fails.

## 8. Modules, packages and secrets

**Q: How do you import a module?** Use `import math`, then call `math.sqrt(16)`. You can also write `from math import sqrt`.

**Q: How do you create your own module?** Save reusable functions in a `.py` file, such as `utils.py`, then write `import utils` (no `.py` extension) in another file.

**Q: How do you install an external module?**

```bash
pip install requests
pip install requests==2.31.0    # a specific version
```

**Q: How do you track the modules a project needs?** List them, optionally with versions, in a `requirements.txt` file in the project root. Anyone setting up the project runs `pip install -r requirements.txt`.

**Q: How do you store secrets such as usernames and passwords securely?**

1. Put them as key–value pairs in a `.env` file in the project root:

   ```
   DB_USER=admin
   DB_PASSWORD=secret123
   ```
2. Install `python-dotenv` (`pip install python-dotenv`), load the file, and read the values with `os.getenv`:

   ```python
   import os
   from dotenv import load_dotenv
   
   load_dotenv()
   password = os.getenv("DB_PASSWORD")
   ```

**Q: How do you keep `.env` out of GitHub?** Add `.env` to the `.gitignore` file in the project root. Git then ignores it on commit and push.

## 9. Error handling and logging

### Exceptions

**Q: How do you handle exceptions?** Put the risky code in a `try` block and handle the failure in an `except` block.

**Q: Can one `try` have several `except` blocks?** Yes. Many candidates get this wrong. Each `except` handles a different exception type.

**Q: What is `finally`?** A `finally` block always runs, whether the code succeeds or fails. Use it for cleanup, such as closing a database connection or a file.

**Q: How do you raise your own exception?** Use `raise` with an exception class and a message.

```python
try:
    result = a / b
except ZeroDivisionError:
    print("Cannot divide by zero")
except TypeError:
    print("Inputs must be numbers")
finally:
    conn.close()                     # always runs

if age < 0:
    raise ValueError("Age cannot be negative")
```

### Logging

**Q: Do you use `print` in production code?** No. Use `print` only while developing. Production code uses the `logging` module with suitable log levels.

**Q: What log levels are there?**

| Level | When to use |
| --- | --- |
| `DEBUG` | Fine-grained detail for debugging |
| `INFO` | Normal progress updates while the program runs |
| `WARNING` | Something to watch that hasn't failed yet. **This is the default level** |
| `ERROR` | Something has failed |
| `CRITICAL` | A serious failure; the program probably can't continue |

In projects the speaker sets the level to `INFO` to track what the process is doing.

**Q: How do you customise logging, for example a log file, level, format and timestamps?** Use `logging.basicConfig`. Put `%(asctime)s` in the format to print the current time on every message.

```python
import logging

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.info("Job started")
```

## 10. Files, OS, dates and JSON

### OS, system and dates

| Question | Answer |
| --- | --- |
| Current working directory | `os.getcwd()` |
| List files and folders | `os.listdir()` |
| Exit the program on a condition | `sys.exit()` |
| Today's date | `datetime.date.today()` |
| Current date and time | `datetime.datetime.now()` |

### Reading and writing files

Use `with open(...)`, which closes the file automatically. The default mode is read (`"r"`). Use `"w"` to write, which overwrites the file, or `"a"` to append.

```python
# read the whole file
with open("data.txt") as file:
    content = file.read()

# write
with open("out.txt", "w") as file:
    file.write("Hello")

# read line by line
with open("data.txt") as file:
    for line in file.readlines():   # readlines() returns a list of lines
        print(line.strip())
```

### JSON

The `s` stands for **string**: `dumps` and `loads` work with strings, while `dump` and `load` work with files.

| Function | Converts | Example |
| --- | --- | --- |
| `json.dumps(d)` | Dictionary to JSON string | `s = json.dumps({"a": 1})` |
| `json.loads(s)` | JSON string to dictionary | `d = json.loads(s)` |
| `json.dump(d, f)` | Dictionary to JSON file | `json.dump(d, file)` |
| `json.load(f)` | JSON file to dictionary | `d = json.load(file)` |

## 11. APIs, SDKs and Streamlit

### Calling APIs with `requests`

| Question | Answer |
| --- | --- |
| How do you call an external API? | `requests.get(url)` |
| How do you read the data from the response? | `response.json()` returns a dictionary |
| How do you pass query parameters? | `requests.get(url, params={"key": "value"})` |
| How do you handle a failed call? | Check `response.status_code`: 200 means success; anything else means failure, so log it or raise an exception |

```python
import requests

response = requests.get("https://api.example.com/users", params={"page": 1})
if response.status_code == 200:
    data = response.json()
else:
    raise Exception(f"API failed: {response.status_code}")
```

### SDKs and GenAI

**Q: Have you used a Python SDK?** Answer from your own experience. The speaker's course uses the **Google Gemini SDK**:

1. Create an API key on Google's portal.
2. Create a `genai` client with the key.
3. Call `generate_content` with a model and your prompt.

**Q: Which GenAI model have you used?** Answer from your own experience. The course uses a fast Gemini Flash model for quick responses.

### Streamlit UI

**Q: Have you built a UI in Python?** Yes: a chatbot built with Streamlit.

| Function | Purpose |
| --- | --- |
| `st.title()` | Sets the app title |
| `st.chat_input()` | Takes input from the user |
| `st.chat_message()` | Creates a chat message bubble |
| `st.write()` | Writes content in the message or on the page |

**Q: How do you run a Streamlit app?** Run `streamlit run app.py`. It prints a local URL that you open in a browser.

**Q: Have you built an AI application?** A sample answer from the video:

> I built a chatbot with the Gemini API and a Streamlit front end. The app takes the user's input, sends it to Gemini, and shows the response in the UI. It also supports multi-turn conversation.

## 12. Last-minute revision

These are the pairs from the video that are easiest to mix up, plus the multiple-\`except\` question the speaker says many candidates miss.

| Commonly confused | Remember |
| --- | --- |
| `break` vs `continue` | `break` ends the loop; `continue` skips only the current iteration |
| List vs set | A list is ordered and allows duplicates; a set is unordered with unique values only |
| List vs tuple | A list is mutable `[]`; a tuple is immutable `()` |
| `dict[key]` vs `dict.get(key)` | `[]` raises `KeyError`; `.get()` returns `None`, which is safer |
| `map` vs `reduce` | `map` returns one output per item; `reduce` returns one combined value |
| `*args` vs `**kwargs` | Positional values in a tuple vs named values in a dictionary |
| `_x` vs `__x` | Protected is only a convention; private is name-mangled and blocks direct access |
| `json.dumps/loads` vs `dump/load` | Versions ending in `s` work with strings; the others work with files |
| Default logging level | `WARNING` (projects typically use `INFO`) |
| Multiple `except` blocks | Allowed. Each block handles a different exception type |
| `finally` | Always runs. Use it to close connections and files |
| Interpreter vs compiler | Python runs code line by line with no executable file |

### Practice checklist

- [ ] Explain the interpreter, `None`, type checking and type conversion
- [ ] Write `range` with 1, 2 and 3 arguments from memory
- [ ] Solve dedup, union, intersection and difference with sets
- [ ] Write a class with `__init__`, a method, a subclass, an override and `super()`
- [ ] Name the 5 inheritance types and 3 access modifiers
- [ ] Write `try` / multiple `except` / `finally` / `raise`
- [ ] Configure `logging.basicConfig` with a file, level and format
- [ ] Set up `.env` + `python-dotenv` + `.gitignore`
- [ ] Call an API with `params` and check `status_code`
- [ ] Describe a project, such as a Gemini + Streamlit chatbot, in your own words
