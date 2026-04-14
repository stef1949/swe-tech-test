# Engineering Spec Exercise — Browser-Based Trace Viewer

## Context

Portal generates electrophysiology recordings from nanopore experiments. A
single recording can contain tens of millions of data points across 48
channels. Scientists need to view and navigate these traces in a web browser
with smooth, real-time pan and zoom.

## The problem

Users need both an overview of the full recording and a high-resolution detail
view of their current viewport. When a user pans or zooms, new data must be
fetched from the server, but the UI should never show blank screens during
transitions. Raw data files live in blob storage. The application needs to run
on a big-3 cloud platform.

## What we're asking for

Explore the problem. We're providing a mock data file for you to work with.
Form a view on what the hard parts are and how you'd solve them. Write up your
thinking as an engineering spec.

We care about your architecture, design, how you'd host this on a big-3 cloud
platform, what risks you see, and how you'd sequence the work if building it
for real. But we care more about the quality of your reasoning than the
completeness of your coverage. Go deep where it matters to you and explain
why.

**We expect deliberate choices about scope. A well-reasoned spec that tackles
the core problem deeply beats one that covers everything shallowly.** You do
not need to design the UI beyond what's necessary to explain your data
delivery approach.

If something you discover in working with the data shapes your thinking, we
want to hear about it. Small, focused experiments that test specific
assumptions are more valuable to us than broad prototypes.

## Example data

The example data is a [Zarr file](https://www.earthmover.io/blog/what-is-zarr/)
containing randomly generated traces that approximate real recording
characteristics: **48 channels sampled at 2.5 kHz over 1.5 hours**.

The file itself (~680 MB) is not included. Generate it locally with
`python generate_mock_recording.py` — see `README.md` for details of the
chunking / sharding / compression layout, which mirrors Portal's
production recording files.

## Format and presentation

Format is up to you: written document, annotated diagrams, a GitHub repo with
a README — whatever communicates your thinking clearly.

You will present your work in a 45-minute call. We will have reviewed your
deliverables beforehand, so plan on roughly 10 to 15 minutes walking us
through your approach. The rest is conversation. Come prepared to talk about
what you found, what surprised you, where you changed your mind, and what
your next steps would be should you continue development.

## Time guidance

Please return your deliverables within 3 days. We expect you to spend a few
hours of focused work. We are assessing the quality of your engineering
thinking, not the volume of your output.

## AI tools

AI tools are welcome and expected. We'll be asking about your design
decisions in the call.
