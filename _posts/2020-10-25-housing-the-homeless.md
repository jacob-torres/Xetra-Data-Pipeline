---
layout: post
title: Housing the Homeless
subtitle: Does Investing in Permanent Housing for the Homeless Significantly Decrease Homelessness?
author: Jacob Torres
gh-repo: jacob-torres/housing-the-homeless/blob/main/Jacob_Torres_DS_Build_Project_1.ipynb
gh-badge: [star, fork, follow]
tags: [data-science, data-analysis, homelessness, housing, poverty, hud]
comments: true
---

I just completed an analysis of the most recent available data on homelessness in the United States, collected and provided by the Department of Housing and Urban Development (HUD.)

Find the data I used in this analysis [here,](https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/), and find the Google Colab notebook [here!](https://colab.research.google.com/drive/1gaTCKCk37ElCcl9m-uFt_rvt_InQ1TXt?usp=sharing)

---

## An Analysis of Investment in Permanent Supportive Housing (PSH) and Change in Homelessness

Homelessness is an unfortunate and difficult problem facing not just the United States, but the entire world. Everywhere on this planet where there are humans, there are humans without homes. It's estimated that there were  567,715 unhoused people living in the United States in January 2019. From 2018 to 2019, homelessness has been trending upward.[1] But as my analysis shows, there's still reason for hope.

The [HUD Exchange](https://www.hudexchange.info/about/#:~:text=The%20HUD%20Exchange%20is%20an,and%20partners%20of%20these%20organizations) is an online platform for the free and open exchange of data on affordable  housing and homelessness. Organizations involved in housing local homeless people, as well as collecting this data, are funded by the department of housing and urban development (HUD.)

The data used in the analysis are the latest data on the HUD Exchange on homelessness in the United States. This includes the average point-in-time count (PIT) and housing inventory count (HIC) from 2007 to 2019.

## Research Question

Does a state's investment in permanent supportive housing (PSH) for the homeless significantly decrease the number of homeless people in that state?

In this analysis, an independent t-test is performed to determine how significant the change in homelessness is based on a state's investment in permanent supportive housing. A result is _significant_ if the p-value falls beneath the significance level of 0.05.

The amount of investment in a certain type of housing project (e.g. permanent housing or emergency shelter) is measured by the number of recorded beds in each category, particularly those that are year-round. In addition, a housing organization's participation in the [Homeless Management Information System](https://www.hudexchange.info/programs/hmis/) (HMIS) may suggest a more standardized approach to targeting homelessness in the area.

## Hypotheses

- Null hypothesis (ho): The amount of investment in permanent supportive housing did _not_ significantly affect the average change in homelessness between 2007 and 2019.
- Alternative hypothesis (ha): The amount of investment in permanent supportive housing _significantly decreased_ the average change in homelessness between 2007 and 2019.

# Data Exploration

In this analysis, I was mostly focused on the average change in PIT count between 2007 and 2019. I wanted to discover how each state's investment in a type of shelter known as [permanent supportive housing](https://www.hudexchange.info/programs/shp/) (PSH) would or would not affect that state's change in homelessness. PSH programs provide supportive, affordable or cost-free housing for individuals and families to help them transition to financial independence.

My initial analysis of the PIT and housing inventory counts revealed a general downward trend in homelessness between 2007 and 2019. The HIC data distinguished between temporary and more permanent types of shelter, as well as whether or not the beds counted in the HIC participated in the HMIS.

I created a coolwarm map of the United States showing the states with a decrease in homelessness, vs those with an increase:

![Coolwarm map of the US](/assets/img/coolwarm_map.png)

The map distinguishes between the states which showed a decrease in homelessness over the given period, vs those that showed an increase. Based on the data, the states with a decrease in homeless claimed the highest number of PSH beds.

In addition, the scatter plot below shows a slight downward trend in homelessness where states participated in the HMIS, which as mentioned above suggests a more standardized approach to homeless sheltering.

![Scatter plot](/assets/img/scatterplot.png)

The p-value in the t-test I performed also confirmed the alternative hypothesis made above, as it fell below the significance level of 0.05.

# Conclusions

Based on the analysis above, I believe that it's reasonable to reject the null (ho,) which states that state investment in PSH shelter for the homelessness does _not_ have a significant effect on the change in homelessness in that state. Clearly there is a positive relationship between investment in PSH shelter and decrease in homelessness between 2007 and 2019, meaning that more state investment in this type of semi-permanent shelter would be advisible if our goal is decreasing homelessness.

Quite obviously, these results force us to ask if the reduction of homelessness is actually our goal or not. If investment in permanent and semi-permanent housing for the homeless ultimately decreases homelessness, then why isn't the United States government interested in making the necessary investments? Why isn't the issue of homelessness mentioned in congress or the media, except to remark on how inconvenient unhoused people are to the rest of us? It feels to me as though Americans and our governmental representatives would like to pretend that the homeless don't exist and hope they go away, but would rather not lift a finger (or tax a giant corporation) in order to create the world we _say_ we want to live in.

Regardless, the ingredients for a successful eradication of homelessness requires that we ask ourselves difficult questions. The data are clear; but are we committed to follow the analysis where it leads us?

[1]: https://endhomelessness.org/homelessness-in-america/homelessness-statistics/state-of-homelessness-2020/
