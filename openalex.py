import json
import requests
import pprint
import pandas as pd

# Create a re-usable function to gather papers and create relational tables
def process_citations(url_list, paper_table, author_table, bridge_table, referenced_works_table, cited_by_table):

    for url in url_list:
        response = requests.get(url)
        data = response.json()

        # print(f"\nData from {url}:")
        # pprint.pprint(data)
        
        # Get the base URL for citing papers
        citing_papers_url = data.get('cited_by_api_url')
        if not citing_papers_url:
            print(f"No citation URL found for {url}")
            continue  # Skip to next URL        
        # Keep track of pagination
        page = 1
        total_pages = 1  # Will be updated in the first request
        
        while page <= total_pages:
            # Add page parameter to URL
            paginated_url = f"{citing_papers_url}&page={page}"
            citing_response = requests.get(paginated_url)
            citing_response.raise_for_status()
            citing_data = citing_response.json()
            
            # Update total pages on first run
            if page == 1:
                total_records = citing_data['meta']['count']
                total_pages = -(total_records // -25)  # Ceiling division
                print(f"Found {total_records} total citations across {total_pages} pages")
            
            for work in citing_data['results']:

                # Only add quality papers
                cited_by_count = work.get('cited_by_count')
                fwci = work.get('fwci')
                if cited_by_count >= 6 and fwci > 1 and work.get('is_retracted', False) is False:
                    
                    # Create main paper entries
                    paper_data = {
                        #'seed_id': data['id'],
                        'paper_id': work.get('id', '').split('/')[-1],
                        'title': work.get('title'),
                        'publication_date': work.get('publication_date'),
                        'url': (work.get('primary_location') or {}).get('landing_page_url'),
                        'open_access': (work.get('open_access') or {}).get('is_oa'),
                        'source': ((work.get('primary_location') or {}).get('source') or {}).get('display_name'),
                        'language': work.get('language'),
                        'cited_by_count': cited_by_count,
                        'fwci': fwci
                    }
                    paper_table = pd.concat([paper_table, pd.DataFrame([paper_data])], ignore_index=True)

                    # Only add authors if we added the paper
                    for author in work.get('authorships', []):
                        author_id = (author.get('author') or {}).get('id').split('/')[-1]
                        # Check if author_id already exists in author_table
                        if author_id not in author_table['author_id'].values: #could improve with else statement!
                            author_data = {
                                "author_id": author_id,
                                "name": (author.get('author') or {}).get('display_name'),
                                "organization": (author.get('institutions', []) or [{}])[0].get('display_name') if author.get('institutions') else None,
                            }
                            author_table = pd.concat([author_table, pd.DataFrame([author_data])], ignore_index=True)

                    # Create author-paper joiner table entries
                    for author in work.get('authorships', []):
                        bridge_data = {
                            "paper_id": paper_data['paper_id'],
                            "author_id": (author.get('author') or {}).get('id', '').split('/')[-1]
                        }
                        bridge_table = pd.concat([bridge_table, pd.DataFrame([bridge_data])], ignore_index=True)

                    #Create referenced_works table entries
                    for referenced_work in work.get('referenced_works', []):
                        referenced_entry = {
                            "paper_id": paper_data['paper_id'],
                            "referenced_work_id": referenced_work.split('/')[-1]
                        }
                        referenced_works_table = pd.concat([referenced_works_table, pd.DataFrame([referenced_entry])], ignore_index=True)

                    # Created cited_by entries
                    cited_by_entry = {
                        "seed_id": url.split('/')[-1],
                        "paper_id": paper_data['paper_id']
                    }
                    cited_by_table = pd.concat([cited_by_table, pd.DataFrame([cited_by_entry])], ignore_index=True)

                    # For future abstract table
                    # abstract_table = {
                    #     'paper_id': paper_data['paper_id'],
                    #     'abstract': work.get('abstract_inverted_index')
                    # }

                    # For future keywords table (maybe merge with abstract table?)
                    # keywords_table = {
                    #     'paper_id': paper_data['paper_id'],
                    #     'keywords': (work.get('keywords')),
                    #     'topics': (work.get('topics'))
                    # }
                    
            print(f"Processed page {page} of {total_pages}")
            page += 1

    return paper_table, author_table, bridge_table, referenced_works_table, cited_by_table

# Initiatilize data frames
paper_table = pd.DataFrame()
author_table = pd.DataFrame(columns=['author_id', 'name', 'organization'])
bridge_table = pd.DataFrame()
referenced_works_table = pd.DataFrame()
cited_by_table = pd.DataFrame()

# Set seed article id
id1 = 'w295038424'
urls = ['https://api.openalex.org/works/' + id1]

# Apply function to the seed article (round 1)
paper_table, author_table, bridge_table, referenced_works_table, cited_by_table= process_citations(
    urls,
    paper_table,
    author_table,
    bridge_table,
    referenced_works_table,
    cited_by_table
)

# Pull all entries from cited_by_table.
# In future, improve by adding entries from referenced_works_table too.
cited_ids = cited_by_table['paper_id'].dropna().unique()
new_urls = [f'https://api.openalex.org/works/{work_id}' 
            for work_id in cited_ids ]

# Apply function to the cited_by_table entries (round 2)
paper_table, author_table, bridge_table, referenced_works_table, cited_by_table= process_citations(
    new_urls,
    paper_table,
    author_table,
    bridge_table,
    referenced_works_table,
    cited_by_table
)

# Write each relational table to Excel workbook
with pd.ExcelWriter('research_data.xlsx') as writer:
    paper_table.to_excel(writer, sheet_name='Papers', index=False)
    author_table.to_excel(writer, sheet_name='Authors', index=False)
    bridge_table.to_excel(writer, sheet_name='Author-Paper Bridge', index=False)
    referenced_works_table.to_excel(writer, sheet_name='References', index=False)
    cited_by_table.to_excel(writer, sheet_name='Citations', index=False)

print('END')