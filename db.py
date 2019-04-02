import json
import os

from fetcher import Profile, Coordinator, Rule

def read_static_data(coordinator, chan_dir):
    files = os.listdir(chan_dir)
    for file in files:
        path = os.path.join(chan_dir, file)
        with open(path, 'r') as f:
            channel_meta = json.load(f)
            p = Profile.from_channel_meta(coordinator, channel_meta)
            assert ('channel-%s.json' % p.ch_id) == file
            coordinator.add_profile(p)

    import get_ia
    ia_ids = get_ia.get_ia_ids()
    for profile in coordinator.get_profiles():
        for vid in profile.get_videos():
            vid.set_in_ia(vid.id in ia_ids)

def print_rules(rules):
    for i, rule in enumerate(rules):
        print('%d:' % i, rule.json())

def run_cmd(coordinator, cmd):
    if cmd == 'reset':
        coordinator.clear()
        read_static_data(coordinator, 'channels')
        coordinator.save_state()
    elif cmd == 'getrules':
        print("Global Rules")
        print_rules(coordinator.get_rules())
        for profile in coordinator.get_profiles():
            rules = profile.get_rules()
            if rules:
                print("Profile rules [%d]" % profile.ch_id)
                print_rules(rules)
    elif cmd.startswith('addrule'):
        args = cmd.split(' ')
        if args[1] == 'global':
            place_to_add = coordinator
        else:
            ch_id = args[1]
            place_to_add = coordinator.profiles[ch_id]
        rule = Rule.parse(args[2:])
        place_to_add.add_rule(rule)
        coordinator.save_state()
    elif cmd.startswith('remrule'):
        where, idx = cmd.split(' ')[1:]
        idx = int(idx)
        if where == 'global':
            place_to_rem = coordinator
        else:
            place_to_rem = coordinator.profiles[where]
        place_to_rem.remove_rule(idx)
        coordinator.save_state()
    elif cmd == 'getdone' or cmd == 'gettodo':
        for profile in coordinator.get_profiles():
            for vid in profile.get_filtered_videos(coordinator.get_rules()):
                if vid.video_data is not None and cmd == 'getdone':
                    print(vid.video_data['filename'])
                elif vid.video_data is None and cmd == 'gettodo':
                    print(vid.id, vid.title)
    elif cmd == 'stat':
        total_size = 0
        count = 0
        total_dur = 0
        for profile in coordinator.get_profiles():
            for vid in profile.get_videos():
                if not vid.video_data:
                    continue
                path = vid.video_data['filename']
                dur = vid.video_data['metadata']['duration']
                size = os.path.getsize(path)
                count += 1
                total_size += size
                total_dur += dur
        def dur_str(seconds):
            return '%d:%d' % divmod(seconds, 60)
        print(('Total size: %.1fGB, count: %d, avg: %.1fMB, Total duration: %s' \
              + ', avg duration: %s')% (
            total_size / 1024 / 1024 / 1024,
            count,
            (total_size / count) / 1024 / 1024, dur_str(total_dur),
            dur_str(total_dur // count)))
    elif cmd.startswith('getvid'):
        id = cmd.split(' ')[1]
        for profile in coordinator.get_profiles():
            for vid in profile.get_videos():
                if vid.id == id:
                    print(json.dumps(vid.video_data, indent=4))
                    break
    elif cmd == 'dedup':
        dirs = {}
        for profile in coordinator.get_profiles():
            for vid in profile.get_videos():
                if vid.video_data is None:
                    continue
                fn = vid.video_data['filename']
                dn, fn = os.path.split(fn)
                dirs.setdefault(dn, []).append((fn, vid))
        changed = False
        for dn, files in dirs.items():
            listfiles = os.listdir(dn)
            for file, vid in files:
                noex = file[:file.rindex('.')]
                adj = [fn for fn in listfiles if fn.startswith(noex)]
                thumbs = []
                subtitles = []
                vidfiles = []
                parts = []
                for f in adj:
                    if f.endswith(".jpg"):
                        thumbs.append(f)
                    elif f.endswith(".vtt"):
                        subtitles.append(f)
                    elif f.endswith(".part"):
                        parts.append(f)
                    else:
                        vidfiles.append(f)
                if len(vidfiles) == 1:
                    continue
                # this is a combined video
                assert '+' in vid.video_data['metadata']['format_id'], vid.id
                nondefault = [fn for fn in vidfiles if not
                              fn.endswith(vid.video_data['metadata']['ext'])]
                # this is the combined file
                assert len(nondefault) == 1, vid.id
                newfile = nondefault[0]
                _, oldfile = os.path.split(vid.video_data['filename'])
                # it must actually be the new one
                assert newfile != oldfile, vid.id
                print("replace", oldfile, "with", newfile)
                # destructive action
                vid.video_data['filename'] = os.path.join(dn, newfile)
                os.remove(os.path.join(dn, oldfile))
                changed = True
        if changed:
            coordinator.save_state()
    elif cmd == 'fixfilename':
        dirs = {}
        for profile in coordinator.get_profiles():
            for vid in profile.get_videos():
                if vid.video_data is None:
                    continue
                fn = vid.video_data['filename']
                dn, fn = os.path.split(fn)
                dirs.setdefault(dn, []).append((fn, vid))
        changed = False
        for dn, files in dirs.items():
            listfiles = os.listdir(dn)
            for file, vid in files:
                if file in listfiles:
                    continue
                noex = file[:file.rindex('.')]
                adj = [fn for fn in listfiles if fn.startswith(noex)]
                assert (noex + '.mkv') in adj
                vid.video_data['filename'] = os.path.join(dn, noex + '.mkv')
                changed = True
        if changed:
            coordinator.save_state()
    else:
        assert False


if __name__ == '__main__':
    coordinator = Coordinator('state.json')
    coordinator.load_state()
    import sys
    if len(sys.argv) > 1:
        run_cmd(coordinator, ' '.join(sys.argv[1:]))
    else:
        while True:
            cmd = input('db > ')
            if cmd in ['q', 'exit', 'quit']:
                break
            run_cmd(coordinator, cmd)
