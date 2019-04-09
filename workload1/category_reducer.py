import sys


def read_map_output(file):
    """ Return an iterator for key, value pair extracted from file (sys.stdin).
    Input format:  key \t value
    Output format: (key, value)
    """
    for line in file:
        yield line.strip().split("\t")


def category_reducer():
    curr_category = ""
    video_countries = {}
    for category, vid, country in read_map_output(sys.stdin):
        # Check if the tag read is the same as the tag currently being processed
        if curr_category != category:

            # If this is the first line (indicated by the fact that current_tag will have the default value of "",
            # we do not need to output tag-owner count yet
            if curr_category != "":
                output = curr_category
                
                average = 0
                for videoId, countries in video_countries.items():
                    output += "{}={}, ".format(videoId, len(set(countries)))
                    average += len(set(countries))*(1/len(video_countries))
                # print(output.strip())
                print("{}:{}".format(output, average))

            # Reset the tag being processed and clear the owner_count dictionary for the new tag
            curr_category = category
            video_countries = {}
        video_countries[vid] = video_countries.get(vid, list())
        video_countries[vid].append(country)

    # We need to output tag-owner count for the last tag. However, we only want to do this if the for loop is called.
    if curr_category != "":
        output = curr_category + "\t"
        print(output)
        average = 0
        for video_id, countries in video_countries.items():
            output += "{}={}, ".format(videoId, len(set(countries)))
            average += len(set(countries)) * (1 / len(video_countries))
        # print(output.strip())
        print(average)


if __name__ == "__main__":
    category_reducer()


