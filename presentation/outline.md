---
title: Presentation outline
author: Thushjandan & François-Xavier
date: May 25th 
--- 

# HB+Trie
## Motivations
* Variable-Length size keys
* Disadvantage in comparision with B+ trees

## Overview

* Key space divided into buckets
* Fixed size chunking of the key
* Create a new B+ tree at every chunk
* Append only disk layout
* Write buffer index

## Implementation

## Performance

* Better than B+ tree with variable size keys

## Possible improvements

* Avoid trie skew with a another type of B+ tree => Leaf B+ tree extension
* Bad Range scan performance

## Discussion/Q&A
