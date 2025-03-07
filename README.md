## Setup Instructions

1. Create a virtual environment:

   - Using bash or zsh:
     python -m venv venv
     source venv/bin/activate

2. Install dependencies in editable mode with development extras:

   - For bash:
     pip install -e .[dev]
   - For zsh:
     pip install -e ".[dev]"

3. Run the application:
   dublin-analysis --property_csv data/dublin-property.csv --tree_json data/dublin-tree.json

4. Run tests:
   pytest
