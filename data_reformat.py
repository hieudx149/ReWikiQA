import json
import re
from tqdm import tqdm


# Use re for split sentence from papragraph
def split_context(context):
    return re.split(r'(?<![A-Z][a-z]\.)(?<![A-Z]\.)(?<=\.)\s(?![0-9])(?![a-z])(?![\(])(?![\{])(?![\[])', context)


# Fix answer start offset
def fix_answer_start(data, v2):
    for entry in tqdm(data):
        for paragraph in entry["paragraphs"]:
            context = paragraph["context"]
            for qa in paragraph["qas"]:
                if v2 is True:
                    if qa["is_impossible"] == False:
                        answer = qa["answers"][0]
                        orig_answer_text = answer["text"]
                        answer_offset = answer["answer_start"]
                        answer_length = len(orig_answer_text)
                        if context[answer_offset - 1: (answer_offset + answer_length - 1)] == orig_answer_text:
                            qa["answers"][0]["answer_start"] = answer_offset - 1
                        elif context[answer_offset + 1: (answer_offset + answer_length + 1)] == orig_answer_text:
                            qa["answers"][0]["answer_start"] = answer_offset + 1
                    else:
                        answer = qa["plausible_answers"][0]
                        orig_answer_text = answer["text"]
                        answer_offset = answer["answer_start"]
                        answer_length = len(orig_answer_text)
                        if context[answer_offset - 1: (answer_offset + answer_length - 1)] == orig_answer_text:
                            qa["plausible_answers"][0]["answer_start"] = answer_offset - 1
                        elif context[answer_offset + 1: (answer_offset + answer_length + 1)] == orig_answer_text:
                            qa["plausible_answers"][0]["answer_start"] = answer_offset - 1
                else:
                    answer = qa["answers"][0]
                    orig_answer_text = answer["text"]
                    answer_offset = answer["answer_start"]
                    answer_length = len(orig_answer_text)
                    if context[answer_offset - 1: (answer_offset + answer_length - 1)] == orig_answer_text:
                        qa["answers"][0]["answer_start"] = answer_offset - 1
                    elif context[answer_offset + 1: (answer_offset + answer_length + 1)] == orig_answer_text:
                        qa["answers"][0]["answer_start"] = answer_offset + 1
    return data


# Split context into sentences and find answer
def fix_answer_and_context(data, v2):
    pair_data = []
    for article in tqdm(data):
        single_article = {"title": article["title"], "paragraphs": []}
        for paragraph in article["paragraphs"]:
            single_data = {"qas": [], "paragraph_contexts": ""}
            # Split paragraph to sentences
            contexts = split_context(paragraph["context"])
            sent_start_idxs = [0]   # Start index of each sentence in the paragraph
            for idx, sentence in enumerate(contexts):
                sent_start_idxs.append(sent_start_idxs[idx] + len(sentence) + 1) # +1 for space
            # Process question-answer pairs
            for qas in paragraph['qas']:
                # Prepare data to save
                if v2 is True:
                    qna_pair = {
                        'question': qas['question'],
                        'label': False if qas['is_impossible'] else True
                    }
                    if len(qas['answers']) != 0 and qas['is_impossible'] is False \
                                and qas['answers'][0]['answer_start'] != -1:
                        answer_start = qas['answers'][0]['answer_start']
                        answer_end = answer_start + len(qas['answers'][0]['text'])
                    elif len(qas['plausible_answers']) != 0 and qas['is_impossible'] is True \
                                and qas['plausible_answers'][0]['answer_start'] != -1:
                        
                        answer_start = qas['plausible_answers'][0]['answer_start']
                        answer_end = answer_start + len(qas['plausible_answers'][0]['text'])
                    else:
                        print("Something wrong !!!")
                        break
                else:
                    qna_pair = {
                        'question': qas['question'],
                        'label': True
                    }
                    if len(qas['answers']) != 0 and qas['answers'][0]['answer_start'] != -1:
                        answer_start = qas['answers'][0]['answer_start']
                        answer_end = answer_start + len(qas['answers'][0]['text']) - 1
                    else:
                        print("Something wrong !!!")
                        break
                # Find the sentence & sentence index that contains the answer
                text = ""
                for idx in range(len(contexts)):
                    
                    curr_start_idx = sent_start_idxs[idx]
                    curr_end_idx = curr_start_idx + len(contexts[idx]) - 1
                    if curr_start_idx <= answer_start <= curr_end_idx and answer_end <= curr_end_idx:
                        text = contexts[idx]
                        break
                    # If answer in many sentences
                    elif curr_start_idx <= answer_start <= curr_end_idx and answer_end > curr_end_idx:
                        text = contexts[idx]
                        start_sent = idx
                        while answer_end > curr_end_idx:
                            idx += 1
                            text += " " + contexts[idx]
                            curr_end_idx += len(contexts[idx]) + 1 # +1 for space
                        end_sent = idx + 1
                        new_contexts = contexts[:start_sent]
                        new_contexts.append(" ".join(contexts[start_sent:end_sent]))
                        new_contexts += contexts[end_sent:]
                        contexts = new_contexts
                        # Update start idx
                        sent_start_idxs = [0]   # Start index of each sentence in the paragraph
                        for idx, sentence in enumerate(contexts):
                            sent_start_idxs.append(sent_start_idxs[idx] + len(sentence) + 1)
                        break
                qna_pair["answer"] = text
                single_data["qas"].append(qna_pair)
                single_data["paragraph_contexts"] = contexts
            single_article["paragraphs"].append(single_data)
        pair_data.append(single_article)
    
    return pair_data


def reformat_context(contexts, window_size=3):
    passages = []
    for start_idx in range(0, len(contexts), window_size):
        end_idx = min(start_idx + window_size, len(contexts))
        if end_idx == start_idx + 1 and len(passages) > 0:
            passages[-1] += " " + contexts[start_idx:end_idx][0]
        else:
            passages.append(" ".join(contexts[start_idx:end_idx]))
    return passages


def fix_context_with_window_size(data):
    for article in data:
        for paragraph in article["paragraphs"]:
            paragraph["paragraph_contexts"] = reformat_context(paragraph["paragraph_contexts"])
    for article in tqdm(data):
        for paragraph in article["paragraphs"]:
            for qa in paragraph["qas"]:
                check = False
                for context in paragraph["paragraph_contexts"]:
                    if qa["answer"] in context:
                        qa["answer"] = context
                        check = True
                        break
                if check is False:
                    print("Something wrong from fix context !!")
    return data


def pipeline_reformat_mrc_dataset(mrc_data):
    mrc_data = fix_answer_start(mrc_data, v2=False)
    mrc_data = fix_answer_and_context(mrc_data, v2=False)
    mrc_data = fix_context_with_window_size(mrc_data)
    return mrc_data


def create_pair_and_corpus(datasets):
    corpus = []
    for name, data in datasets.items():
        print(f"Processing {name} dataset !!!")
        save_pairs = []
        for article in tqdm(data):
            article_pairs = {
                "title": article["title"],
                "qas": []
            }
            article_corpus = {
                "title": article["title"],
                "contexts": []
            }
            for paragraph in article["paragraphs"]:
                for qa in paragraph["qas"]:
                    if qa not in article_pairs["qas"]:
                        article_pairs["qas"].append(qa)
                for context in paragraph["paragraph_contexts"]:
                    if context not in article_corpus["contexts"]:
                        article_corpus["contexts"].append(context)
            save_pairs.append(article_pairs)
            corpus.append(article_corpus)
        with open(f"data/{name}.json", "w", encoding="utf-8") as f:
            json.dump(save_pairs, f, ensure_ascii=False, indent=4)
    
    with open(f"data/corpus.json", "w", encoding="utf-8") as f:
            json.dump(corpus, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":

    with open("ViQuAD/train_ViQuAD.json", "r", encoding="utf-8") as f:
        train_viquad = json.load(f)["data"]
    with open("ViQuAD/dev_ViQuAD.json", "r", encoding="utf-8") as f:
        dev_viquad = json.load(f)["data"]
    with open("ViQuAD/test_ViQuAD.json", "r", encoding="utf-8") as f:
        test_viquad = json.load(f)["data"]
    with open("ViQuAD/dev-context-vi-question-vi.json", "r", encoding="utf-8") as f:
        dev_mlqa = json.load(f)["data"]
    with open("ViQuAD/test-context-vi-question-vi.json", "r", encoding="utf-8") as f:
        test_mlqa = json.load(f)["data"]
    
    train_viquad_reformat = pipeline_reformat_mrc_dataset(train_viquad)
    dev_viquad_reformat = pipeline_reformat_mrc_dataset(dev_viquad)
    test_viquad_reformat = pipeline_reformat_mrc_dataset(test_viquad)
    dev_mlqa_reformat = pipeline_reformat_mrc_dataset(dev_mlqa)
    test_mlqa_reformat = pipeline_reformat_mrc_dataset(test_mlqa)

    mrc_datasets = {"train": train_viquad_reformat + test_mlqa_reformat, "dev": dev_viquad_reformat, "test": test_viquad_reformat + dev_mlqa_reformat}
    create_pair_and_corpus(mrc_datasets)