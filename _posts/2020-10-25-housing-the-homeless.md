---
layout: post
title: Housing the Homeless
subtitle: Does Investing in Permanent Housing for the Homeless Significantly Decrease Homelessness?
author: Jacob A. Torres
gh-repo: jacob-torres/housing-the-homeless/blob/main/Jacob_Torres_DS_Build_Project_1.ipynb
gh-badge: [star, fork, follow]
tags: [data-science, data-analysis, homelessness, housing, poverty, hud]
comments: true
---

I just completed an analysis of the most recent available data on homelessness in the United States, collected and provided by the Department of Housing and Urban Development (HUD.)

Find the data I used in this analysis [here,](https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/)

and find the Google Colab notebook [here!](https://colab.research.google.com/drive/1gaTCKCk37ElCcl9m-uFt_rvt_InQ1TXt?usp=sharing)


# Housing the Homeless
## An Analysis of Investment in Permanent Supportive Housing (PSH) and Change in Homelessness

Homelessness is an unfortunate and difficult problem facing not just the United States, but the entire world. Everywhere on this planet where there are humans, there are humans without homes. It's estimated that there were  567,715 homeless people living in the United States in January 2019. Since 2007, homelessness has been trending upward.[1]

The [HUD Exchange](https://www.hudexchange.info/about/#:~:text=The%20HUD%20Exchange%20is%20an,and%20partners%20of%20these%20organizations) is an online platform for the free and open exchange of data on affordable  housing and homelessness. Organizations involved in housing local homeless people, as well as collecting this data, are funded by the department of housing and urban development (HUD.)

Below are the latest data on the HUD Exchange on homelessness in the United States. This includes the average point-in-time count (PIT) and housing inventory count (HIC) from 2007 to 2019. Each dataset is in excel format.

## Research Question

Does a state's investment in permanent supportive housing (PSH) for the homeless significantly decrease the number of homeless people in that state?

In this analysis, an independent t-test is performed to determine how significant the change in homelessness is based on a state's investment in permanent supportive housing. A result is _significant_ if the p-value falls beneath the significant level of 0.05.

The amount of investment in a certain type of housing project (e.g. permanent housing or emergency shelter) is measured by the number of recorded beds in each category, particularly those that are year-round. In addition, a housing organization's participation in the [Homeless Management Information System](https://www.hudexchange.info/programs/hmis/) (HMIS) may suggest a more standardized approach to targeting homelessness in the area.

## Hypotheses

- Null hypothesis (ho): The amount of investment in permanent supportive housing did _not_ significantly affect the average change in homelessness between 2007 and 2019.
- Alternative hypothesis (ha): The amount of investment in permanent supportive housing _significantly decreased_ the average change in homelessness between 2007 and 2019.

[1]: https://endhomelessness.org/homelessness-in-america/homelessness-statistics/state-of-homelessness-2020/
